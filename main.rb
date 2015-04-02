require 'rubygems'
require 'bundler'

Bundler.require
require 'thread/pool'
require 'pp'

module EdgeHistory

    MONGO = Mongo::Client.new(['127.0.0.1:27017'], :database => 'edge-history')
    CONFIG = JSON.parse(File.read("sites_config.json")).merge(JSON.parse(File.read("rules_config.json")))

    class HistoryUpdater
        def initialize()
            @rules = CONFIG["rules"]
            @data_col = MONGO['data']
            @data_col.indexes.create_one({"date" => 1}, unique: true)
        end

        def process(days = 1)
            date = Date.today
            (1..days).each do |n|
                puts n
                puts "Processing #{date - n}"
                process_day(date - n)
            end
        end

        def process_day(date = Date.today - 1)
            
            from = date
            from = Time.utc(from.year, from.month, from.day, 0, 0, 0)
            to = date + 1
            to = Time.utc(to.year, to.month, to.day, 0, 0, 0)

            pool = Thread.pool(10)
            data = {}
            datalock = Mutex.new

            @rules.each do |rule|
                pool.process do
                    eval_rule(rule, from, to).each do |site, vals|
                        datalock.synchronize do
                            data[site] ||= {}
                            data[site].merge! vals
                        end
                    end
                end
            end

            pool.shutdown

            record = {}
            record["from"] = from.iso8601(3)
            record["to"] = to.iso8601(3)
            record["date"] = from
            record["weekend"] = from.saturday? or from.sunday?
            record["data"] = data

            @data_col.find("date" => record["date"]).update_one(record, {upsert: true})
        end

        def eval_rule(rule, from, to)

            bindings = {}
            results = {}

            rule["bindings"].each do |name, body|
                eval_binding(body, from, to).each do |site, binding|
                    bindings[site] ||= {}
                    bindings[site][name] = binding
                end
            end

            bindings.each do |site, b|
                results[site] ||= {}
                results[site][rule["name"]] = eval(rule["definition"])
            end

            results
        end

        def eval_binding(binding, from, to)

            results = {}

            CONFIG["sites"].each do |site|
                conn = Faraday.new(url: site["url"]) do |fara|
                    fara.basic_auth(CONFIG["auth"]["user"], CONFIG["auth"]["password"])
                    fara.adapter(Faraday.default_adapter)
                    #fara.response :logger
                end
                response = conn.get do |req|
                    req.url "#{site["url"]}/search/universal/absolute/fieldhistogram"
                    req.params["Content-Type"] = "application/json"
                    req.params["query"] = binding["query"]
                    req.params["field"] = binding["field"]
                    req.params["interval"] = "day"
                    req.params["from"] = from.iso8601(3)
                    req.params["to"] = to.iso8601(3)
                end

                results[site["name"]] = JSON.parse(response.body)["results"].first[1]
                results[site["name"]].each do |name, val|
                    results[site["name"]][name] = Float(val)
                end
            end

            combined = {
                "min" => results.first[1]["min"],
                "max" => results.first[1]["max"],
                "count" => 0.0,
                "total_count" => 0.0,
                "total" => 0.0
            }

            results.each do |site, value|
                combined["min"] = [combined["min"], value["min"]].min
                combined["max"] = [combined["max"], value["max"]].max
                combined["total_count"] += value["total_count"]
                combined["count"] += value["count"]
                combined["total"] += value["total"]
            end

            combined["mean"] = combined["total"] / combined["count"]
            results["All"] = combined

            results
        end
    end

    class Grapher
        def initialize()
            @output_file = "output.png"
            @data_col = MONGO['data']
        end

        def generate_graph(name, opt = {})
            data, sites = gather_data(name)
            colors = CONFIG["sites"].map{|site| site["color"]} + ["black"]

            opt = DEFAULT_OPTS.merge(opt)
            g = nil

            case opt["type"]
            when "line"
                g = Gruff::Line.new(opt["size"])
                g.line_width = 1
                g.dot_radius = 0.5
            when "area"
                g = Gruff::StackedArea.new(opt["size"])
                sites.delete("All")
            end

            if opt.has_key? "maximum_value"
                g.maximum_value = opt["maximum_value"]
            end
            if opt.has_key? "minimum_value"
                g.minimum_value = opt["minimum_value"]
            end

            g.title = name
            g.theme = {
                colors: colors,
                marker_color: "black",
                background_colors: "white"
            }

            n = -1
            g.labels = data.map do |date, datum|
                n += 1
                {n => date.strftime("%b %e")}
            end.reduce do |h1, h2|
                h1.merge h2
            end

            pp g.labels

            sites.each do |site|
                site_data = []
                data.each do |date, datum|
                    if datum.has_key? site
                        site_data.push datum[site]
                    else
                        site_data.push nil
                    end
                end
                pp site_data
                g.data(site, site_data)
            end

            g.write(@output_file)
        end
                

        def gather_data(name)
            data = {}
            sites = Set.new

            @data_col.find.sort("date" => 1).each do |record|
                data[record["date"]] = {}
                record["data"].each do |site, cols|
                    data[record["date"]][site] = cols[name]
                    sites.add site
                end
            end

            [data, sites]
        end

        DEFAULT_OPTS = {
            "type" => "line",
            "size" => "1800x900"
        }
        PERCENT_OPTS = {
            "type" => "line",
            "maximum_value" => 1.0,
            "minimum_value" => 0.0
        }
        TOTAL_OPTS = {
            "type" => "area"
        }
    end
end

#hu = EdgeHistory::HistoryUpdater.new()
#hu.process(5)
#
hg = EdgeHistory::Grapher.new()
hg.generate_graph("Dedicated Fax Success Rate", EdgeHistory::Grapher::PERCENT_OPTS)
