local cjson = require("cjson")
local luasql = require("luasql.odbc")
local lfs = require("lfs")

local sql_server = "sqlddatabasedemo.database.windows.net"
local sql_db_name = "SampleDB"
local sql_username = "sqladmin"
local sql_password = "Reviveyourbody47"

local input_file_path = "/data/dataout"

local env = luasql.odbc()
local conn = env:connect(sql_db_name, sql_username, sql_password, sql_server)

if not conn then
    print("Database connection failed!")
    os.exit(1)
end

local function process_json_data(file_path)
    local data_list = {}
    for line in io.lines(file_path) do
        if line ~= "" then
            local ok, json_data = pcall(cjson.decode, line)
            if ok then
                for _, item in ipairs(json_data["Apollo"] or {}) do
                    table.insert(data_list, item)
                end
            else
                print("Skipping invalid JSON line: " .. line)
            end
        end
    end
    return data_list
end

local function aggregate_data(data_list)
    local grouped_data = {}

    for _, record in ipairs(data_list) do
        local meter = record["Meter"]
        local datetime = os.date("%Y-%m-%d %H:%M", os.time(record["RecOn"])) -- 转为15分钟间隔
        local key = meter .. "_" .. datetime

        if not grouped_data[key] then
            grouped_data[key] = {
                Meter = meter,
                DateTime = datetime,
                kWh_IMP = record["kWh_IMP"] or 0,
                kWh_EXP = record["kWh_EXP"] or 0,
                kvarh_IMP = record["kvarh_IMP"] or 0,
                kvarh_EXP = record["kvarh_EXP"] or 0,
                kVAh = record["kVAh"] or 0,
                V = record["V"] or 0,
                I = record["I"] or 0,
                kW = record["kW"] or 0,
                I_THD = record["I_THD"] or 0,
            }
        else
            local group = grouped_data[key]
            group.kWh_IMP = math.max(group.kWh_IMP, record["kWh_IMP"] or 0)
            group.kWh_EXP = math.max(group.kWh_EXP, record["kWh_EXP"] or 0)
            group.kvarh_IMP = math.max(group.kvarh_IMP, record["kvarh_IMP"] or 0)
            group.kvarh_EXP = math.max(group.kvarh_EXP, record["kvarh_EXP"] or 0)
            group.kVAh = math.max(group.kVAh, record["kVAh"] or 0)
            group.V = math.max(group.V, record["V"] or 0)
            group.I = math.max(group.I, record["I"] or 0)
            group.kW = math.max(group.kW, record["kW"] or 0)
            group.I_THD = math.max(group.I_THD, record["I_THD"] or 0)
        end
    end

    local result = {}
    for _, v in pairs(grouped_data) do
        table.insert(result, v)
    end
    return result
end

local function upload_to_database(data_list, table_name)
    local insert_query = string.format(
        "INSERT INTO %s (Meter, DateTime, kWh_IMP, kWh_EXP, kvarh_IMP, kvarh_EXP, kVAh, V, I, kW, I_THD) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        table_name
    )

    for _, record in ipairs(data_list) do
        local stmt = conn:prepare(insert_query)
        stmt:execute(
            record.Meter,
            record.DateTime,
            record.kWh_IMP,
            record.kWh_EXP,
            record.kvarh_IMP,
            record.kvarh_EXP,
            record.kVAh,
            record.V,
            record.I,
            record.kW,
            record.I_THD
        )
    end

    print("Data uploaded successfully.")
end

local function main()
    local table_name = "ApolloTesting"
    print("Processing JSON data...")
    local data_list = process_json_data(input_file_path)
    print("Aggregating data...")
    local aggregated_data = aggregate_data(data_list)
    print("Uploading data to database...")
    upload_to_database(aggregated_data, table_name)
    print("All tasks completed!")
end

main()

conn:close()
env:close()
