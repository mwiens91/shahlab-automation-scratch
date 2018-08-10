

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('tag_name')
    parser.add_argument('from_storage_name')
    parser.add_argument('to_storage_name')
    args = vars(parser.parse_args())

    do_transfer(
        args['tag_name'],
        args['from_storage_name'],
        args['to_storage_name'],
    )

