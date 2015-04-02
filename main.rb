require 'rubygems'
require 'bundler'

Bundler.require
require 'thread/pool'
require 'pp'

require_relative 'updater'
require_relative 'grapher'

module LogHistory

    MONGO = Mongo::Client.new(['127.0.0.1:27017'], :database => 'log-history')
    CONFIG = JSON.parse(File.read("sites_config.json")).merge(JSON.parse(File.read("rules_config.json")))

    class CLI < Thor
        desc "update", "Update from all rules, opt # of days to update."
        def update(num = 1)
            puts "Updating the last #{num} days."
            updater = Updater.new
            updater.process(num.to_i)
        end

        desc "graph_line", "Graphs a rule as a line."
        def graph_line(name, outfile = "output.png")
            puts "Graphing #{name}... (line)"
            grapher = Grapher.new outfile
            grapher.generate_graph(name, Grapher::LINE_OPTS)
        end

        desc "graph_rate", "Graphs a rule as a percentile."
        def graph_rate(name, outfile = "output.png")
            puts "Graphing #{name}... (%)"
            grapher = Grapher.new outfile
            grapher.generate_graph(name, Grapher::PERCENT_OPTS)
        end

        desc "graph_area", "Graphs a rule as an area."
        def graph_area(name, outfile = "output.png")
            puts "Graphing #{name}... (area)"
            grapher = Grapher.new outfile
            grapher.generate_graph(name, Grapher::AREA_OPTS)
        end
    end
end

LogHistory::CLI.start
