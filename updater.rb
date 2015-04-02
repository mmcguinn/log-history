module LogHistory
    class Updater
        def initialize()
            @rules = CONFIG["rules"]
            @data_col = MONGO['data']
            @data_col.indexes.create_one({"date" => 1}, unique: true)
        end

        def process(days = 1)
            date = Date.today
            (1..days).each do |n|
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
                    p 'Evaling rule'
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
                #puts 'Evaling binding'
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
end
