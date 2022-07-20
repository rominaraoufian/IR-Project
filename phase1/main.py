from information_retrieval.constructor import BSBIIndex

DATASET_PATH = 'E:\Dataset_IR\Train'
OUTPUT_DIR = 'E:\Dataset_IR\Train'


if __name__ == '__main__':
    BSBI_instance = BSBIIndex(data_dir=DATASET_PATH, output_dir=OUTPUT_DIR)
    query = input('search >>  ')
    result = BSBI_instance.retrieve(query)
    print(result)