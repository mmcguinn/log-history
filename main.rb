require 'rubygems'
require 'bundler'

Bundler.require
require 'thread/pool'
require 'pp'

require_relative 'updater'
require_relative 'grapher'

module LogHistory

    MONGO = Mongo::Client.new(['127.0.0.1:27017'], :database => 'edge-history')
    CONFIG = JSON.parse(File.read("sites_config.json")).merge(JSON.parse(File.read("rules_config.json")))

    class CLI < Thor
        desc "update", "Update from all rules, opt # of days to update."
        def update(num = 1)
            puts "Updating the last #{num} days."
            updater = Updater.new
            updater.process(num.to_i)
        end
    end
end

#hu = LogHistory::Updater.new()
#hu.process(5)

#hg = LogHistory::Grapher.new()
#hg.generate_graph("Dedicated Fax Success Rate", LogHistory::Grapher::PERCENT_OPTS)

LogHistory::CLI.start
