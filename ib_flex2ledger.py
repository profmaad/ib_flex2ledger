from lxml import etree
import json
import requests
import sys
from time import sleep
import typer
from typing_extensions import Annotated
from dataclasses import dataclass

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

app = typer.Typer()

def retrieve_flex(config: Config, wait_seconds: int) -> str:
    print("Executing statement generation...", end='', file=sys.stderr)
    send_request_parameters = {
        'v': 3,
        't': config.api_token,
        'q': config.query_id,
        }
    send_request_response = requests.get('https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest', params=send_request_parameters)
    send_request_result = etree.fromstring(send_request_response.text)

    if send_request_result.xpath('//FlexStatementResponse/Status/text()')[0] != 'Success':
        raise ValueError(f"Response to /SendRequest was not Success:\n{send_request_response.text}")

    reference_code = send_request_result.xpath('//FlexStatementResponse/ReferenceCode/text()')[0]
    print(f"done: {reference_code}", file=sys.stderr)

    print(f"Waiting {wait_seconds} seconds for statement to finish generating...", end='', file=sys.stderr)
    sleep(wait_seconds)
    print("done", file=sys.stderr)

    print("Retrieving generated statement...", end='', file=sys.stderr)
    get_statement_parameters = {
    'v': 3,
    't': config.api_token,
    'q': reference_code,
    }
    get_statement_response = requests.get('https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement', params=get_statement_parameters)
    print("done", file=sys.stderr)

    return get_statement_response.text

@app.command()
def retrieve_flex_command(config_file: Annotated[str, typer.Option()], wait_seconds: int = 5):
    config = Config.load(config_file)
    print(retrieve_flex(config, wait_seconds))

if __name__ == "__main__":
    app()