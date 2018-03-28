re 'csv'
require 'json'
require 'elasticsearch'

@fields_schema = {
    crime_type: {type: String, required: true},
    crime_subtype: {type: String, required: true},
    loss_amount: {type: Numeric, required: false, default: 0.0},
    address_street: {type: String, required: false, default: ''},
    address_neighborhood: {type: String, required: false, default: ''},
    address_municipality: {type: String, required: false, default: ''},
    address_zipcode: {type: String, required: false, default: ''},
    address_state: {type: String, required: false, default: ''},
    latitude: {type: Numeric, required: true},
    longitude: {type: Numeric, required: true},
    description: {type: String, required: true, format: /^$/},
    date: {type: String, required: true, format: /^$/}
}

@inserted_records = []

def red(str)
    "\e[31m#{str}\e[0m"
end

def logger(type, field, value = nil)
    result = case type
        when :required then "#{field.to_s} was not present"
        when :type then "#{field.to_s} of incorrect type, the value was #{value}"
        else 'Invalid type of log'
    end

    puts red("  [error] #{result}")
end

def import_incidents_from_file(filename)
    # total number of csv records
    total_lines = %x{wc -l < "#{filename}"}.to_i
    errors = []
    success_number = 0

    CSV.foreach(filename, headers: true, header_converters: :symbol) do |row|
        puts "[*] Processing record #{$. - 1}/#{total_lines}"
        # puts row.to_hash
        if insert_incident(row.to_hash)
            success_number = success_number + 1
        else
            errors << $.
        end
    end

    failures_message = red("#{errors.size} failures")
    puts "Summary: #{total_lines} records processed, #{success_number} correct, #{failures_message}"
    if errors.any?
        puts "  The errors ocurred on the following lines: #{red(errors.join(', '))}"
    end
end

def insert_incident(incident)
    if is_valid?(incident)
        # response = @client.index(index: 'incidents', type: 'incident', body: incident)
        # raise "Problems inserting incident #{incident.to_s}" if response['result'] != 'created'
        @inserted_records << incident.values
        true
    else
        puts red("  [x] Line number #{$.} has an invalid format, record wasn't saved")
        false
    end
end

def is_valid?(record)
    # check that all of the fields on the record are valid
    return false unless @fields_schema.all? { |key, tests| pass?(record, key, tests) }

    true
end

def pass?(hash_, key, rules)
    # check that that the value is present if it's a required field
    if rules[:required] && hash_[key].nil?
        logger(:required, key)
        return false
    end

    # set default value for optional field
    hash_[key] = rules[:default] if hash_[key].nil?

    # parse numeric fields
    if rules[:type] == Numeric
        hash_[key] = hash_[key].to_f
    end

    # check that the field is of the correct type
    unless hash_[key].is_a?(rules[:type])
        logger(:required, key, hash_[key])
    end

    # otherwise the record is valid
    true
end

incidents_file = './seeds/incidents.csv'
url = 'http://localhost:9200'

unless File.file?(incidents_file)
  puts "Error: #{incidents_file} file doesn't exist"
  exit
end

@client = Elasticsearch::Client.new(url: url, log: true)

import_incidents_from_file(incidents_file)

CSV.open('./seeds/processed_incidents.csv', 'wb') do |csv|
    @inserted_records.each do |row|
        csv << row
    end
end
