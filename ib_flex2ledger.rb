#!/usr/bin/env ruby
# typed: true

require 'rubygems'
require 'nokogiri'
require 'commander/import'
require 'pp'
require 'net/http'
require 'uri'
require 'csv'

program :name, 'ib_flex2ledger'
program :version, '0.1'
program :description, 'Parse Interactive Brokers Flex reports into ledger transactions'

def load(file)
  File.open(file) do |f|
    Nokogiri::XML(f)
  end
end

def transaction_header(trade)
  puts "#{trade["settleDateTarget"]}=#{trade["tradeDate"]} * #{trade["exchange"]}"
  puts "  ; trade_id: #{trade["tradeID"]}"
  puts "  ; order_id: #{trade["ibOrderID"]}"
end

def fees_to_ledger(trade, options)
  puts "  #{options.cash_account}  #{trade["ibCommissionCurrency"]} #{trade["ibCommission"].to_f}"
  puts "  #{options.fees_account}  #{trade["ibCommissionCurrency"]} #{-trade["ibCommission"].to_f}"
end

def stock_trade_to_ledger(trade, options)
  transaction_header(trade)
  puts "  #{options.stock_account}  \"#{trade["symbol"]}\" #{trade["quantity"].to_f}"
  puts "  #{options.cash_account}  #{trade["currency"]} #{trade["proceeds"].to_f}"
  fees_to_ledger(trade, options)
  puts ""
end

def fx_trade_to_ledger(trade, options)
  base_currency, quote_currency = trade["symbol"].split('.')

  transaction_header(trade)
  puts "  #{options.cash_account}  #{base_currency} #{trade["quantity"].to_f}"
  puts "  #{options.cash_account}  #{quote_currency} #{trade["proceeds"].to_f}"
  fees_to_ledger(trade, options)
  puts ""
end

def group_cash_transactions(cash_transactions)
  cash_transactions.reduce({}) do |hash, transaction|
    key = {conid: transaction["conid"], date: transaction["dateTime"]}

    if hash[key].nil? then
      hash[key] = [transaction]
    else
      hash[key] << transaction
    end

    hash
  end
end

def classify_cash_transactions_group(transaction_group)
  grouped = transaction_group.group_by {|transaction| transaction["type"]}

  results = []

  if grouped.fetch("Dividends",[]).length == 1 and grouped.fetch("Withholding Tax",[]).length == 1 then
    results << {type: :dividend_with_withholding, dividend: grouped["Dividends"].first, tax: grouped["Withholding Tax"].first}
    grouped.delete("Dividends")
    grouped.delete("Withholding Tax")
  end

  grouped.each do |key, transactions|
    transactions.each do |transaction|
      results << {type: key.to_sym, transaction: transaction}
    end
  end

  results
end

def retrieve_flex(api_token, query_id, wait_seconds)
  STDERR.print "Executing statement generation..."
  send_request_parameters = {
    'v': 3,
    't': api_token,
    'q': query_id,
  }

  send_request_uri = URI.parse('https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest')
  send_request_uri.query = URI.encode_www_form(send_request_parameters)
  send_request_response = Net::HTTP.get_response(send_request_uri)
  send_request_result = Nokogiri::XML(send_request_response.body)

  unless send_request_result.xpath('//FlexStatementResponse/Status').text == 'Success' then
    raise RuntimeError.new("Response to /SendRequest was not Success:\n#{send_request_response.body}")
  end

  reference_code = send_request_result.xpath('//FlexStatementResponse/ReferenceCode').text
  STDERR.puts "done: #{reference_code}"

  STDERR.print "Waiting #{wait_seconds} seconds for statement to finish generating..."
  sleep(20)
  STDERR.puts "done"

  STDERR.print "Retrieving generated statement..."
  get_statement_parameters = {
    'v': 3,
    't': api_token,
    'q': reference_code,
  }

  get_statement_uri = URI.parse('https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement')
  get_statement_uri.query = URI.encode_www_form(get_statement_parameters)
  get_statement_response = Net::HTTP.get_response(get_statement_uri)

  STDERR.puts("done")

  return get_statement_response.body
end

def parse_trades_from_flex(flex_report, options)
  statement = flex_report.xpath("//FlexStatement").first
  account_info = statement.xpath("AccountInformation").first
  $stderr.puts "Trades for #{account_info["name"]}, account #{statement["accountId"]}"
  $stderr.puts "Period #{statement["fromDate"]} to #{statement["toDate"]}"

  if options.new_only
    latest_transaction_date = get_latest_transaction_date_from_hledger(options.stock_account)
    $stderr.puts "Dropping transactions older than #{latest_transaction_date}"
  else
    latest_transaction_date = DateTime.new()
  end

  trades = statement.xpath("Trades/Trade")
  trades.sort_by {|trade| DateTime.parse("#{trade["tradeDate"]} #{trade["tradeTime"]}")}.each do |trade|
    next if DateTime.parse(trade["tradeDate"]) <= latest_transaction_date

    case trade["assetCategory"]
    when "CASH" then fx_trade_to_ledger(trade, options)
    when "STK"  then stock_trade_to_ledger(trade, options)
    else
      say_error "Dropping trade with unknown assetCategory=#{trade["assetCategory"]}"
      say_error trade.inspect
    end
  end

  cash_transactions = group_cash_transactions(statement.xpath("CashTransactions/CashTransaction[@levelOfDetail='DETAIL']"))
  cash_transactions.sort_by {|key, _| key[:date]}.each do |key, transactions|
    next if DateTime.parse(key[:date]) <= latest_transaction_date

    grouped_transactions = classify_cash_transactions_group(transactions)
    grouped_transactions.each do |transaction|
      case transaction[:type]
      when :dividend_with_withholding
        dividend = transaction[:dividend]
        tax = transaction[:tax]

        puts "#{dividend["reportDate"]} * #{dividend["symbol"]}"
        puts "  ; #{dividend["description"]}"
        puts "  #{options.dividends_account}  #{dividend["currency"]} #{-dividend["amount"].to_f}"
        puts "  #{options.withholdings_account}  #{tax["currency"]} #{-tax["amount"].to_f}"
        puts "  #{options.cash_account}"
        puts ""
      when :Dividends
        dividend = transaction[:transaction]

        puts "#{dividend["reportDate"]} * #{dividend["symbol"]}"
        puts "  ; #{dividend["description"]}"
        puts "  #{options.dividends_account}  #{dividend["currency"]} #{-dividend["amount"].to_f}"
        puts "  #{options.cash_account}"
        puts ""
      when :"Broker Interest Received"
        transaction = transaction[:transaction]

        puts "#{transaction["reportDate"]} * Interactive Brokers"
        puts "  ; #{transaction["description"]}"
        puts "  #{options.interest_income_account}  #{transaction["currency"]} #{-transaction["amount"].to_f}"
        puts "  #{options.cash_account}"
        puts ""
      when :"Broker Interest Paid"
        transaction = transaction[:transaction]

        puts "#{transaction["reportDate"]} * Interactive Brokers"
        puts "  ; #{transaction["description"]}"
        puts "  #{options.interest_expense_account}  #{transaction["currency"]} #{-transaction["amount"].to_f}"
        puts "  #{options.cash_account}"
        puts ""
      when :"Other Fees"
        transaction = transaction[:transaction]

        puts "#{transaction["reportDate"]} * Interactive Brokers"
        puts "  ; #{transaction["description"]}"
        puts "  #{options.fees_account}  #{transaction["currency"]} #{-transaction["amount"].to_f}"
        puts "  #{options.cash_account}"
        puts ""
      when :"Deposits/Withdrawals"
        next if options.ignore_deposits_withdrawals
        transaction = transaction[:transaction]

        puts "#{transaction["reportDate"]} * UNKNOWN"
        puts "  ; #{transaction["description"]}"
        puts "  #{options.cash_account}  #{transaction["currency"]} #{transaction["amount"].to_f}"
        puts "  UNKNOWN_ACCOUNT"
        puts ""
      else
        transaction = transaction[:transaction]
        amount = transaction["amount"].to_f

        puts "#{transaction["reportDate"]} * Interactive Brokers"
        puts "  ; #{transaction["description"]}"
        puts "  ; cash_transaction_type: #{transaction["type"]}"
        puts "  #{options.cash_account}  #{transaction["currency"]} #{transaction["amount"].to_f}"
        if amount < 0 then
          puts "  #{options.fees_account}"
        else
          puts "  UNKNOWN_ACCOUNT"
        end
        puts ""
      end
    end
  end
end

def get_latest_transaction_date_from_hledger(stock_account)
  begin
    transactions_csv = %x[hledger aregister -O csv --date2 '#{stock_account}']
    transactions = CSV.parse(transactions_csv, headers:true)
    return DateTime.parse(transactions[-1]['date'])
  rescue
    return DateTime.new()
  end 
end

command :parse_trades do |c|
  c.syntax = 'ib_flex2ledger parse-transactions [options]'
  c.summary = 'Parse trades and output ledger transactions'
  c.description = ''
  c.option '--stock-account STRING', String, 'Ledger account to be used for stock positions'
  c.option '--cash-account STRING', String, 'Ledger account to be used for cash positions'
  c.option '--fees-account STRING', String, 'Ledger account to be used for fees'
  c.option '--dividends-account STRING', String, 'Ledger account to be used for dividends income'
  c.option '--withholdings-account STRING', String, 'Ledger account to be used for withholding tax'
  c.option '--interest-income-account STRING', String, 'Ledger account to be used for interest income'
  c.option '--interest-expense-account STRING', String, 'Ledger account to be used for interest expenses'
  c.option '--ignore-deposits-withdrawals', 'If specified, don\'t output transactions for deposits and withdrawals'
  c.option '--new-only', 'If specified, don\'t output transactions older than the latest recorded ledger transaction on the stock account'
  c.action do |args, options|
    raise ArgumentError.new("--stock-account is required") if options.stock_account.nil?
    options.default :cash_account => options.stock_account,
                    :fees_account => "Expenses:Fees:Brokerage",
                    :dividends_account => "Income:Dividends",
                    :withholdings_account => "Expenses:Taxes:US Withholding Tax",
                    :interest_income_account => "Income:Interest",
                    :interest_expense_account => "Expenses:Interest"

    flex_report = load(args[0])
    parse_trades_from_flex(flex_report, options)
  end
end

command :retrieve_and_parse do |c|
  c.syntax = 'ib_flex2ledger retrieve_and_parse [options]'
  c.summary = 'Retrieve Flex report via API, parse trades from it and output ledger transactions'
  c.description = ''
  c.option '--stock-account STRING', String, 'Ledger account to be used for stock positions'
  c.option '--cash-account STRING', String, 'Ledger account to be used for cash positions'
  c.option '--fees-account STRING', String, 'Ledger account to be used for fees'
  c.option '--dividends-account STRING', String, 'Ledger account to be used for dividends income'
  c.option '--withholdings-account STRING', String, 'Ledger account to be used for withholding tax'
  c.option '--interest-income-account STRING', String, 'Ledger account to be used for interest income'
  c.option '--interest-expense-account STRING', String, 'Ledger account to be used for interest expenses'
  c.option '--ignore-deposits-withdrawals', 'If specified, don\'t output transactions for deposits and withdrawals'
  c.option '--api-token STRING', String, 'Authentication token for the IBKR Flex webservice'
  c.option '--query-id STRING', String, 'Query ID of the Flex query to execute'
  c.option '--wait-seconds INT', Integer, 'Seconds to sleep between executing the query and retrieving the result (default: 5s)'
  c.option '--new-only', 'If specified, don\'t output transactions older than the latest recorded ledger transaction on the stock account'
  c.action do |args, options|
    raise ArgumentError.new("--stock-account is required") if options.stock_account.nil?
    options.default :cash_account => options.stock_account,
                    :fees_account => "Expenses:Fees:Brokerage",
                    :dividends_account => "Income:Dividends",
                    :withholdings_account => "Expenses:Taxes:US Withholding Tax",
                    :interest_income_account => "Income:Interest",
                    :interest_expense_account => "Expenses:Interest",
                    :wait_seconds => 5

    flex = retrieve_flex(options.api_token, options.query_id, options.wait_seconds)
    flex_report = Nokogiri::XML(flex)
    parse_trades_from_flex(flex_report, options)
  end
end

command :print_positions do |c|
  c.syntax = 'ib_flex2ledger print-positions [options]'
  c.summary = 'Print current positions'
  c.description = ''
  c.option '--verbose', 'Include more information than just basic positions'
  c.action do |args, options|
    flex_report = load(args[0])
    statement = flex_report.xpath("//FlexStatement").first
    account_info = statement.xpath("AccountInformation").first
    puts "Positions for #{account_info["name"]}, account #{statement["accountId"]}"
    puts "As per #{statement["toDate"]}"
    puts ""

    assets = statement.xpath("OpenPositions/OpenPosition[@levelOfDetail='SUMMARY']")
    assets.each do |position|
      currency = position["currency"]

      puts "#{position["symbol"]} #{position["position"]}"

      if options.verbose then
        puts " * Percent of NAV: #{position["percentOfNAV"]}%"
        puts " * Cost basis price: #{position["openPrice"]} #{currency}"
        puts " * M2M price: #{position["markPrice"]} #{currency}"
        puts " * Cost basic value : #{position["costBasisMoney"]} #{currency}"
        puts " * M2M value: #{position["positionValue"]} #{currency}"
        puts " * M2M PnL: #{position["fifoPnlUnrealized"]} #{currency}"
        puts ""
      end
    end

    fx = statement.xpath("FxPositions/FxPosition[@levelOfDetail='SUMMARY']")
    fx.each do |position|
      puts "#{position["fxCurrency"]} #{position["quantity"]}"
    end
  end
end

command :dividends_to_csv do |c|
  c.syntax = 'ib_flex2ledger dividends-to-csv [options]'
  c.summary = 'Parse dividends only and output csv'
  c.description = ''
  c.action do |args, options|
    flex_report = load(args[0])
    statement = flex_report.xpath("//FlexStatement").first
    account_info = statement.xpath("AccountInformation").first
    $stderr.puts "Dividends for #{account_info["name"]}, account #{statement["accountId"]}"
    $stderr.puts "Period #{statement["fromDate"]} to #{statement["toDate"]}"

    puts "date,symbol,amount,tax"

    cash_transactions = group_cash_transactions(statement.xpath("CashTransactions/CashTransaction"))
    cash_transactions.sort_by {|key, _| key[:date]}.each do |key, transactions|
      classify_cash_transactions_group(transactions).each do |transaction|
        case transaction[:type]
        when :dividend_with_withholding
          dividend = transaction[:dividend]
          tax = transaction[:tax]

          amount_without_tax = dividend["amount"].to_f + tax["amount"].to_f

          puts "#{dividend["reportDate"]},#{dividend["symbol"]},#{amount_without_tax},#{-tax["amount"].to_f}"
        end
      end
    end
  end
end

command :retrieve_flex do |c|
  c.syntax = 'ib_flex2ledger retrieve-flex [options]'
  c.summary = 'Execute a Flex query via the IBKR Flex webservice and retrieve the result'
  c.description = 'See https://www.interactivebrokers.com/campus/ibkr-api-page/flex-web-service/ for documentation  and setup'
  c.option '--api-token STRING', String, 'Authentication token for the IBKR Flex webservice'
  c.option '--query-id STRING', String, 'Query ID of the Flex query to execute'
  c.option '--wait-seconds INT', Integer, 'Seconds to sleep between executing the query and retrieving the result (default: 5s)'
  c.action do |args, options|
    options.default :wait_seconds => 5

    raise ArgumentError.new("--api-token is required") if options.api_token.nil?
    raise ArgumentError.new("--query-id is required") if options.query_id.nil?

    flex = retrieve_flex(options.api_token, options.query_id, options.wait_seconds)
    puts(flex)
  end
end