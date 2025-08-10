from lxml import etree
import json
import requests
import sys
from time import sleep
import typer
from datetime import datetime
from collections import defaultdict
from typing import Any
from typing_extensions import Annotated
from dataclasses import dataclass
from functools import reduce
import subprocess
import csv
from io import StringIO


@dataclass(frozen=True)
class Config:
    # Ledger asset account to be used for stock positions
    stock_account: str
    # Ledger asset account to be used for cash positions
    cash_account: str
    # Ledger expenses account to be used for fees
    fees_account: str
    # Ledger revenue account to be used for dividends income
    dividends_account: str
    # Ledger expenses account to be used for withholding tax
    withholdings_account: str
    # Ledger revenue account to be used for interest income
    interest_income_account: str
    # Ledger expenses account to be used for interest expenses
    interest_expense_account: str

    # Authentication token for the IBKR Flex webservice
    api_token: str
    # Query ID of the Flex query to execute
    query_id: str

    @classmethod
    def load(cls, config_file: str):
        config_json = json.load(open(config_file))
        return cls(**config_json)


def transaction_header(trade: dict[str, Any]) -> None:
    print(
        f"{trade.get("settleDateTarget")}={trade.get("tradeDate")} * {trade.get("exchange")}"
    )
    print(f"  ; trade_id: {trade.get("tradeID")}")
    print(f"  ; order_id: {trade.get("ibOrderID")}")


def fees_to_ledger(trade: dict[str, Any], config: Config) -> None:
    print(
        f"  {config.cash_account}  {trade.get("ibCommissionCurrency")} {float(trade.get("ibCommission"))}"
    )
    print(
        f"  {config.fees_account}  {trade.get("ibCommissionCurrency")} {-float(trade.get("ibCommission"))}"
    )


def stock_trade_to_ledger(trade: dict[str, Any], config: Config) -> None:
    transaction_header(trade)
    print(
        f'  {config.stock_account}  "{trade.get("symbol")}" {float(trade.get("quantity"))}'
    )
    print(
        f"  {config.cash_account}  {trade.get("currency")} {float(trade.get("proceeds"))}"
    )
    fees_to_ledger(trade, config)
    print("")


def fx_trade_to_ledger(trade: dict[str, Any], config: Config) -> None:
    base_currency, quote_currency = trade.get("symbol").split(".")

    transaction_header(trade)
    print(f"  {config.cash_account}  {base_currency} {float(trade.get("quantity"))}")
    print(f"  {config.cash_account}  {quote_currency} {float(trade.get("proceeds"))}")
    fees_to_ledger(trade, config)
    print("")


def group_cash_transactions(cash_transactions: list[dict]) -> dict:
    def reduce_fn(hash, transaction):
        key = (transaction.get("conid"), transaction.get("dateTime"))
        if key not in hash:
            hash[key] = [transaction]
        else:
            hash[key].append(transaction)
        return hash

    return reduce(reduce_fn, cash_transactions, {})


def classify_cash_transactions_group(transaction_group):
    grouped = defaultdict(list)
    for transaction in transaction_group:
        grouped[transaction.get("type")].append(transaction)

    results = []

    if len(grouped["Dividends"]) == 1 and len(grouped["Withholding Tax"]) == 1:
        results.append(
            {
                "type": "dividend_with_withholding",
                "dividend": grouped["Dividends"][0],
                "tax": grouped["Withholding Tax"][0],
            }
        )
        del grouped["Dividends"]
        del grouped["Withholding Tax"]

    for key, transactions in grouped.items():
        for transaction in transactions:
            results.append({"type": key, "transaction": transaction})

    return results


app = typer.Typer()


def get_latest_transaction_date_from_hledger(account: str) -> datetime:
    try:
        transactions_csv = subprocess.run(
            ["hledger", "aregister", "-O", "csv", "--date2", account],
            capture_output=True,
            text=True,
        )
        transactions = list(csv.DictReader(StringIO(transactions_csv.stdout)))
        return datetime.fromisoformat(transactions[-1]["date"])
    except Exception:
        return datetime.min


def retrieve_flex(config: Config, wait_seconds: int) -> str:
    print("Executing statement generation...", end="", file=sys.stderr)
    send_request_parameters = {
        "v": 3,
        "t": config.api_token,
        "q": config.query_id,
    }
    send_request_response = requests.get(
        "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest",
        params=send_request_parameters,
    )
    send_request_result = etree.fromstring(send_request_response.text)

    if (
        send_request_result.xpath("//FlexStatementResponse/Status/text()")[0]
        != "Success"
    ):
        raise ValueError(
            f"Response to /SendRequest was not Success:\n{send_request_response.text}"
        )

    reference_code = send_request_result.xpath(
        "//FlexStatementResponse/ReferenceCode/text()"
    )[0]
    print(f"done: {reference_code}", file=sys.stderr)

    print(
        f"Waiting {wait_seconds} seconds for statement to finish generating...",
        end="",
        file=sys.stderr,
    )
    sleep(wait_seconds)
    print("done", file=sys.stderr)

    print("Retrieving generated statement...", end="", file=sys.stderr)
    get_statement_parameters = {
        "v": 3,
        "t": config.api_token,
        "q": reference_code,
    }
    get_statement_response = requests.get(
        "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement",
        params=get_statement_parameters,
    )
    print("done", file=sys.stderr)

    return get_statement_response.text


def parse_trades_from_flex(
    flex_report, new_only: bool, ignore_deposits_withdrawals: bool, config: Config
) -> None:
    statement = flex_report.xpath("//FlexStatement")[0]
    account_info = statement.xpath("AccountInformation")[0]
    print(
        f"Trades for {account_info.get("name")}, account {statement.get("accountId")}",
        file=sys.stderr,
    )
    print(
        f"Period {statement.get("fromDate")} to {statement.get("toDate")}",
        file=sys.stderr,
    )

    if new_only:
        latest_transaction_date = get_latest_transaction_date_from_hledger(
            config.stock_account
        )
        print(
            f"Dropping transactions older than {latest_transaction_date}",
            file=sys.stderr,
        )
    else:
        latest_transaction_date = datetime.min

    trades = statement.xpath("Trades/Trade")
    trades = sorted(
        trades, key=lambda trade: datetime.fromisoformat(trade.get("dateTime"))
    )
    for trade in trades:
        if datetime.fromisoformat(trade.get("tradeDate")) <= latest_transaction_date:
            continue

        match trade.get("assetCategory"):
            case "CASH":
                fx_trade_to_ledger(trade, config)
            case "STK":
                stock_trade_to_ledger(trade, config)
            case _:
                print(
                    f"Dropping trade with unknown assetCategory={trade.get("assetCategory")}",
                    file=sys.stderr,
                )
                print(trade, file=sys.stderr)

    cash_transactions = group_cash_transactions(
        statement.xpath("CashTransactions/CashTransaction[@levelOfDetail='DETAIL']")
    )
    cash_transactions = sorted(cash_transactions.items(), key=lambda item: item[0][1])
    for key, transactions in cash_transactions:
        if datetime.fromisoformat(key[1]) <= latest_transaction_date:
            continue

        grouped_transactions = classify_cash_transactions_group(transactions)
        for transaction in grouped_transactions:
            match transaction["type"]:
                case "dividend_with_withholding":
                    dividend = transaction["dividend"]
                    tax = transaction["tax"]

                    print(f"{dividend.get("reportDate")} * {dividend.get("symbol")}")
                    print(f"  ; {dividend.get("description")}")
                    print(
                        f"  {config.dividends_account}  {dividend.get("currency")} {-float(dividend.get("amount"))}"
                    )
                    print(
                        f"  {config.withholdings_account}  {tax.get("currency")} {-float(tax.get("amount"))}"
                    )
                    print(f"  {config.cash_account}")
                    print(f"")
                case "Dividends":
                    dividend = transaction["transaction"]

                    print(f"{dividend.get("reportDate")} * {dividend.get("symbol")}")
                    print(f"  ; {dividend.get("description")}")
                    print(
                        f"  {config.dividends_account}  {dividend.get("currency")} {-float(dividend.get("amount"))}"
                    )
                    print(f"  {config.cash_account}")
                    print(f"")
                case "Broker Interest Received":
                    transaction = transaction["transaction"]

                    print(f"{transaction.get("reportDate")} * Interactive Brokers")
                    print(f"  ; {transaction.get("description")}")
                    print(
                        f"  {config.interest_income_account}  {transaction.get("currency")} {-float(transaction.get("amount"))}"
                    )
                    print(f"  {config.cash_account}")
                    print(f"")
                case "Broker Interest Paid":
                    transaction = transaction["transaction"]

                    print(f"{transaction.get("reportDate")} * Interactive Brokers")
                    print(f"  ; {transaction.get("description")}")
                    print(
                        f"  {config.interest_expense_account}  {transaction.get("currency")} {-float(transaction.get("amount"))}"
                    )
                    print(f"  {config.cash_account}")
                    print(f"")
                case "Other Fees":
                    transaction = transaction["transaction"]

                    print(f"{transaction.get("reportDate")} * Interactive Brokers")
                    print(f"  ; {transaction.get("description")}")
                    print(
                        f"  {config.fees_account}  {transaction.get("currency")} {-float(transaction.get("amount"))}"
                    )
                    print(f"  {config.cash_account}")
                    print(f"")
                case "Deposits/Withdrawals":
                    if ignore_deposits_withdrawals:
                        continue
                    transaction = transaction["transaction"]

                    print(f"{transaction.get("reportDate")} * UNKNOWN")
                    print(f"  ; {transaction.get("description")}")
                    print(
                        f"  {config.cash_account}  {transaction.get("currency")} {float(transaction.get("amount"))}"
                    )
                    print(f"  UNKNOWN_ACCOUNT")
                    print(f"")
                case _:
                    transaction = transaction["transaction"]
                    amount = float(transaction.get("amount"))

                    print(f"{transaction.get("reportDate")} * Interactive Brokers")
                    print(f"  ; {transaction.get("description")}")
                    print(f"  ; cash_transaction_type: {transaction.get("type")}")
                    print(
                        f"  {config.cash_account}  {transaction.get("currency")} {amount}"
                    )
                    if amount < 0:
                        print(f"  {config.fees_account}")
                    else:
                        print(f"  UNKNOWN_ACCOUNT")
                    print(f"")


@app.command("retrieve-flex")
def retrieve_flex_command(
    config_file: Annotated[str, typer.Option()], wait_seconds: int = 5
) -> None:
    config = Config.load(config_file)
    print(retrieve_flex(config, wait_seconds))


@app.command("parse-trades")
def parse_trades_command(
    flex_file: str,
    config_file: Annotated[str, typer.Option()],
    new_only: bool = False,
    ignore_deposits_withdrawals: bool = False,
) -> None:
    config = Config.load(config_file)
    with open(flex_file, "rb") as f:
        flex_report = etree.parse(f).getroot()
        parse_trades_from_flex(
            flex_report, new_only, ignore_deposits_withdrawals, config
        )


if __name__ == "__main__":
    app()
