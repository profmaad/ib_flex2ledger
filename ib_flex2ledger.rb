#!/usr/bin/env ruby

require 'rubygems'
require 'nokogiri'
require 'commander/import'
require 'pp'

program :name, 'ib_flex2ledger'
program :version, '0.0.1'
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
  puts "  #{options.stock_account}  #{trade["symbol"]} #{trade["quantity"].to_f}"
  puts "  #{options.cash_account}  #{trade["currency"]} #{trade["proceeds"].to_f}"
  fees_to_ledger(trade, options)
  puts ""
end

def fx_trade_to_ledger(trade, options)
  base_currency, quote_currency = trade["symbol"].split('.')
  buy_currency, sell_currency = if trade["buySell"] == "BUY" then
                                  [base_currency, quote_currency]
                                else
                                  [quote_currency, base_currency]
                                end

  transaction_header(trade)
  puts "  #{options.cash_account}  #{buy_currency} #{trade["quantity"].to_f}"
  puts "  #{options.cash_account}  #{sell_currency} #{trade["proceeds"].to_f}"
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

command :parse_trades do |c|
  c.syntax = 'ib_flex2ledger parse-transactions [options]'
  c.summary = 'Parse trades and output ledger transactions'
  c.description = ''
  c.option '--stock-account STRING', String, 'Ledger account to be used for stock positions'
  c.option '--cash-account STRING', String, 'Ledger account to be used for cash positions'
  c.option '--fees-account STRING', String, 'Ledger account to be used for fees'
  c.option '--dividends-account STRING', String, 'Ledger account to be used for dividends income'
  c.option '--withholdings-account STRING', String, 'Ledger account to be used for withholding tax'
  c.action do |args, options|
    raise ArgumentError.new("--stock-account is required") if options.stock_account.nil?
    options.default :cash_account => options.stock_account,
                    :fees_account => "Expenses:Fees:Brokerage",
                    :dividends_account => "Income:Dividends",
                    :withholdings_account => "Expenses:Taxes:US Withholding Tax"

    flex_report = load(args[0])
    statement = flex_report.xpath("//FlexStatement").first
    account_info = statement.xpath("AccountInformation").first
    $stderr.puts "Trades for #{account_info["name"]}, account #{statement["accountId"]}"
    $stderr.puts "Period #{statement["fromDate"]} to #{statement["toDate"]}"

    trades = statement.xpath("Trades/Trade")
    trades.sort_by {|trade| DateTime.parse("#{trade["tradeDate"]} #{trade["tradeTime"]}")}.each do |trade|
      case trade["assetCategory"]
      when "CASH" then fx_trade_to_ledger(trade, options)
      when "STK"  then stock_trade_to_ledger(trade, options)
      else
        say_error "Dropping trade with unknown assetCategory=#{trade["assetCategory"]}"
        say_error trade.inspect
      end
    end

    cash_transactions = group_cash_transactions(statement.xpath("CashTransactions/CashTransaction"))
    cash_transactions.sort_by {|key, _| key[:date]}.each do |key, transactions|
      classify_cash_transactions_group(transactions).each do |transaction|
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
