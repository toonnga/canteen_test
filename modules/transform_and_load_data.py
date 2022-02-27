import pandas as pd
import configparser


class TransformAndLoadData:

    def __init__(self, config_file):
        self.config_file = config_file
        self.donations_detail_path = ""
        self.mapping_table_path = ""
        self.destination_path = ""
        self.df_donations_detail = pd.DataFrame()
        self.df_mapping_table = pd.DataFrame()
        self.join_data = pd.DataFrame()

    def __str__(self):
        output = f"""
config_file : {self.table_name}
donations_detail_path : {self.project_name}
mapping_table_path : {self.time_delta}
destination_path : {self.destination_path}"""
        return output

    def read_configfile(self) -> None:
        config_path = f'./config/{self.config_file}.properties'
        parser = configparser.ConfigParser()
        parser.read(config_path)
        config = parser['default']
        self.donations_detail_path = config['donations_detail_path']
        self.mapping_table_path = config['mapping_table_path']
        self.destination_path = config['destination_path']

    def read_csv_file(self):
        self.df_donations_detail = pd.read_csv(self.donations_detail_path, dtype='unicode')
        self.df_mapping_table = pd.read_csv(self.mapping_table_path)
        return self.df_donations_detail, self.df_mapping_table

    def change_column_name_donation(self):
        self.df_donations_detail = self.df_donations_detail.rename(columns={'AAKPAY__BANKED_AMOUNT__C': 'Payment_Amount',
                                                                  'AAKPAY__CAMPAIGN_NAME__C': 'Appeal_Code',
                                                                  'AAKPAY__CONTACT__C': 'URN',
                                                                  'AAKPAY__METHOD_OF_PAYMENT__C': 'Payment_Method',
                                                                  'AAKPAY__TRANSACTION_DATE__C': 'Payment_Date',
                                                                  'ID': 'Gift_ID',
                                                                  'MARVIN_CONTACT_ID__C': 'Legacy_ID',
                                                                  'OPPORTUNITY_RECORD_TYPE__C': 'Transaction_Type',
                                                                  'START_ACQUISITION_TYPE__C': 'Acquisition_Type'})
        return self.df_donations_detail

    def change_column_name_mapping_table(self):
        self.df_mapping_table = self.df_mapping_table.rename(columns={'START_ACQUISITION_TYPE__C': 'Acquisition_Type',
                                                            'Gift Type': 'Gift_Type',
                                                            'Solicitation Channel': 'Solicitation_Channel'})

        return self.df_mapping_table

    def change_data_type_donations(self) -> None:
        self.df_donations_detail['Payment_Amount'] = self.df_donations_detail.Payment_Amount.astype('float64')
        self.df_donations_detail['Appeal_Code'] = self.df_donations_detail.Appeal_Code.astype('string')
        self.df_donations_detail['URN'] = self.df_donations_detail.URN.astype('string')
        self.df_donations_detail['Payment_Method'] = self.df_donations_detail.Payment_Method.astype('string')
        self.df_donations_detail['Payment_Date'] = pd.to_datetime(self.df_donations_detail['Payment_Date'],
                                                                  format='%Y-%m-%d').dt.strftime('%d/%m/%Y')
        self.df_donations_detail['Gift_ID'] = self.df_donations_detail.Gift_ID.astype('string')
        self.df_donations_detail['Legacy_ID'] = self.df_donations_detail.Legacy_ID.astype('string')
        self.df_donations_detail['Transaction_Type'] = self.df_donations_detail.Transaction_Type.astype('string')
        self.df_donations_detail['Acquisition_Type'] = self.df_donations_detail.Acquisition_Type.astype('string')

    def change_data_type_mapping_table(self) -> None:
        self.df_mapping_table['Acquisition_Type'] = self.df_mapping_table.Acquisition_Type.astype('string')
        self.df_mapping_table['Gift_Type'] = self.df_mapping_table.Gift_Type.astype('string')
        self.df_mapping_table['Solicitation_Channel'] = self.df_mapping_table.Solicitation_Channel.astype('string')

    def check_length_data(self) -> None:
        self.df_donations_detail['URN_length'] = self.df_donations_detail['URN'].map(str).apply(len)
        self.df_donations_detail['Gift_ID_length'] = self.df_donations_detail['Gift_ID'].map(str).apply(len)

        for index, row in self.df_donations_detail.iterrows():
            if row['URN_length'] != 18:
                print("URN length is not 18 characters -> ", row['URN'], "=", row['URN_length'])
            if row['Gift_ID_length'] != 18:
                print("URN length is not 18 characters -> ", row['Gift_ID'], "=", row['Gift_ID_length'])

    def join_df(self):
        self.join_data = self.df_donations_detail.merge(self.df_mapping_table, on='Acquisition_Type', how='left')
        return self.join_data

    def load_data_to_csv(self, result) -> None:
        result.to_csv(self.destination_path, sep=',', encoding='utf-8', index=False)

    def load_source_to_target(self) -> None:
        self.read_configfile()
        self.read_csv_file()

        self.change_column_name_donation()
        self.change_data_type_donations()
        self.check_length_data()

        self.change_column_name_mapping_table()
        self.change_data_type_mapping_table()

        data = self.join_df()
        result = data[['URN', 'Gift_ID', 'Payment_Date', 'Payment_Amount',
                       'Payment_Method', 'Appeal_Code', 'Gift_Type', 'Solicitation_Channel']]

        self.load_data_to_csv(result)
