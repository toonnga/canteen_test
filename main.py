import sys
from modules.transform_and_load_data import TransformAndLoadData

if __name__ == '__main__':

    config_file = sys.argv[1]

    donations_result_table = TransformAndLoadData(config_file)
    donations_result_table.load_source_to_target()

