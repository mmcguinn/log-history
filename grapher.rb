module LogHistory
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
