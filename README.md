# canteen_test
Data Engineer Test - Load CSV and Transform data

# LOAD CSV FILE AND TRANSFORM DATA
====

This repository is load data from csv file, transform and give a result to csv file.

## Step for Run Code

    1. please create folder /sources and folder /result before
    2. download 2 files from https://www.dropbox.com/sh/666ota0ypdraggv/AACwuHi-VJbcezVm1tBsMshea?dl=0
    3. rename "1 Donations_Detail.csv" to "Donations_Detail.csv"
    4. rename "2 Mapping_Table.csv" to "Mapping_Table.csv"
    5. place 2 files in step 3,4 to /sources folder 
    6. run ./build.sh for build docker image
    7. run ./run_script.sh for run docker image
    8. you will see the result in csv file at result folder

## Detail in Code

* source: csv file (path: /sources/Donations_Detail.csv, /sources/Mapping_Table.csv)
* output: csv file (path: /result/Donations_Result.csv)
* path: /config
    you can add config file whatever you want in the same format of csv_config_file.properties, if you have some another files that you want to load and transform (just change the value in config file)
        - donations_detail_path: source file path
        - mapping_table_path=source file path (mapping file path)
        - destination_path= destination path
* path: /modules
    - transform_and_load_data.py: have a class "TransformAndLoadData" use for read csv file (sources path) > transform data > load to csv in another path (result path)

        - variable in class
            <pre>  self.config_file: config file name <str>
            self.donations_detail_path: source file path <str>
            self.mapping_table_path: source file path (mapping file path) <str>
            self.destination_path: destination path <str>
            self.df_donations_detail = dataframe of source <df>
            self.df_mapping_table = dataframe of mapping table <df>
            self.join_data = dataframe of data after join <df> </pre>

        - __str__ method: just for print config variable

        - read_configfile method: use for read config from config file to variable in class

        - read_csv_file method: read csv file and dump data to dataframe

        - change_column_name_donation method: change column name for donation table

        - change_column_name_mapping_table method: change column name for mapping table

        - change_data_type_donations method: change data type for donation table

        - change_data_type_mapping_table method: change data type for mapping_table

        ```** ps. if you have new table and new structure, you have to create new method for change column name and change data type. **```

        - check_length_data method: check for some columns following requirement that is URN and Gift_ID should have 18 characters (but just print to show what record is not 18 characters)

        - join_df method: join 2 tables (in left join way)

        - load_data_to_csv method: load result data to csv file (path: /result)

        - load_source_to_target: compound all methods by call every methods in one method that is load config file > read csv file > change_column_name_donation > change_data_type_donations > check_length_data > change_column_name_mapping_table > change_data_type_mapping_table > join dataframe > load_data_to_csv
