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

    cash_transactions = statement.xpath("CashTransactions/CashTransaction")
    cash_transactions.sort_by {|tx| DateTime.parse(tx["dateTime"])}.each do |transaction|
      puts "#{transaction["reportDate"]} * #{transaction["symbol"]}"
      puts "  ; #{transaction["description"]}"
      puts ""
    end
  end
end
