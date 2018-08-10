import os
import time
import logging
import coreapi
from openapi_codec import OpenAPICodec
from coreapi.codecs import JSONCodec
from collections import defaultdict

# import tantalus_utils

log = logging.getLogger('sisyphus')

TANTALUS_API_USER = 'amcpherson'#os.environ['TANTALUS_API_USER']
TANTALUS_API_PASSWORD = '$p0tLuck!'#os.environ['TANTALUS_API_PASSWORD']
TANTALUS_DOCUMENT_URL = os.environ.get('TANTALUS_DOCUMENT_URL', 'http://127.0.0.1:8000/api/swagger/?format=openapi')

auth = coreapi.auth.BasicAuthentication(
    username = TANTALUS_API_USER,
    password = TANTALUS_API_PASSWORD
)
decoders = [OpenAPICodec(), JSONCodec()]

CLIENT = coreapi.Client(auth = auth, decoders = decoders)
SCHEMA = CLIENT.get(TANTALUS_DOCUMENT_URL, format = 'openapi')

### GENERIC TASKS ###
def make_tantalus_query(table, params, suppress_messages = False):
    """ Query tantalus with pagination
    """

    params = dict(params)
    params['limit'] = 100
    params['offset'] = 0

    data = []
    next = 'next'

    while next is not None:
        r = CLIENT.action(SCHEMA, table, params = params)
        if not suppress_messages:
            log.debug('Querying table: {} with params: {}'.format(table, params))

        if r['count'] == 0:
            raise Exception('No results for {}'.format(params))

        data.extend(r['results'])

        if 'next' in r:
            next = r['next']
        else:
            next = None

        params['offset'] += params['limit']

    return data


def create_resource(get_table, post_table, body, unique_identifiers = []):
    """Create a resource in tantalus.

    This method checks to see if a resource already exists.
    If it does not exist the resource is created and the
    primary key returned, otherwise the primary key of the
    existing resource is returned

    Args:
        get_table: the endpoint of the tantalus database resource
        post_table: the endpoint to add a new resource
        body: a dictionary containing all the information to
            create a record in tantalus
        unique_identifies:keys in the body that make
            the element unique

    Returns: The primary key of the resource
    """
    # Get the resource filtered by the unique identifiers to determine if it already exists 
    params = {}
    for uid in unique_identifiers:
        params[uid] = body[uid]

    log.debug("Querying {} with {}".format(get_table, body))
    r = CLIENT.action(SCHEMA, get_table, params = params)

    if (r['count'] == 0) or (unique_identifiers == []):
        # No unique prexisting resource, create a new one
        log.debug("Creating resource {} with {}".format(post_table, body))
        p = CLIENT.action(SCHEMA, post_table, params = body)
        id = p['id']
    elif r['count'] == 1:
        # There is an existing resource, grab its primary key
        id = r['results'][0]['id']
    else:
        # Something has gone horribly wrong
        log.error("TABLE:", get_table)
        log.error("BODY:", body)
        log.error("UIDS:", unique_identifiers)
        log.error("RESULTS:", r['results'])
        raise Exception("Too many results")
        
    return id


def query_SimpleTask_for_status(table, params):
    """Queries the status of a sample task"""
    g = CLIENT.action(SCHEMA, table, params = params)
    return g['results'][0]


def wait_for_finish(table, task_ids):
    """Waits for tasks to finish"""
    log.debug("Waiting for tasks with IDs {} at endpoint {} to finish".format(
        task_ids, table))
    for task in task_ids:
        param = {'id': task}
        log.debug("Checking task with ID {}".format(task))
        status = query_SimpleTask_for_status(table, {'id': task})
        while not status["finished"]:
            time.sleep(10)
            status = query_SimpleTask_for_status(table, {'id': task})
        if status["finished"] and not status["success"]:
            log.error("SimpleTask with ID {} failed.".format(task))
            raise Exception("Task failed")

### SPECIFIC TASKS ###

def query_gsc_dlp_fastqs(dlp_library_id, gsc_library_id):
    """ Run a query for GSC DLP fastqs """
    params = {
        'dlp_library_id': dlp_library_id,
        'gsc_library_id': gsc_library_id,
    }
    get_table = ['queries', 'gsc_dlp_paired_fastqs_list']
    post_table = ['queries', 'gsc_dlp_paired_fastqs_create']
    query_id = [create_resource(get_table, post_table, params, ['dlp_library_id'])]
    wait_for_finish(get_table, query_id)
    return query_id[0]


def get_storage(storage_name):
    """ Get a storage by name and return directory and ID """
    if not storage_name:
        raise Exception('Must specify name of storage.')

    storage = make_tantalus_query(['storage', 'generic_list'], {'name': storage_name}, suppress_messages = True)

    if len(storage) != 1:
        raise Exception('Found {} storages with name {}'.format(len(storage), storage))

    return storage[0]


def get_storage_id_and_dir(storage_name):
    storage = make_tantalus_query(['storage', 'generic_list'], {'name': storage_name}, suppress_messages = True)

    if len(storage) != 1:
        raise Exception('Found {} storages with name {}'.format(len(storage), storage))

    if 'storage_directory' in storage[0].keys():
        return storage[0]['id'], storage[0]['storage_directory']
    return storage[0]['id'], {'storage_account': storage[0]['storage_account'], 'storage_container': storage[0]['storage_container']}


def get_sequence_datasets(library_id=None, sample_id=None):
    table = ['sequence_dataset', 'list']
    params = {}

    if library_id is not None:
        params['library__library_id'] = library_id
    if sample_id is not None:
        params['sample__sample_id'] = sample_id

    return make_tantalus_query(table, params)


def get_file_resource(resource_id):
    return make_tantalus_query(['file_resource', 'list'], params = {'id': resource_id}, suppress_messages = True)[0]


def get_pairedend_fastqs(location=None, library_id=None, sample_id=None, index_sequences=None, 
                            requested_lane_ids=None):
    sequence_datasets = set()
    file_resources = set()
    lanes_dict = {}

    for dataset in get_sequence_datasets(library_id, sample_id):
        if not tantalus_utils.sequence_dataset_match_type(dataset, 'FQ'):
            continue
        if len(tantalus_utils.get_lanes_from_dataset(dataset)) != 1:
            raise Exception('Sequence dataset {} has {} lanes'.format(dataset, 
                tantalus_utils.get_lanes_from_dataset(dataset)))
        if not tantalus_utils.fastq_sequence_dataset_match_lane(dataset, requested_lane_ids):
            continue
        sequence_datasets.add(dataset['id'])
        file_resources.update(dataset['file_resources'])

        for resource in dataset['file_resources']:
            lanes_dict[resource] = {'lane_id': tantalus_utils.get_lanes_from_dataset(dataset).pop(),
                                    'sequencing_centre': tantalus_utils.get_sequencing_centre_from_dataset(dataset),
                                    'sequencing_instrument': tantalus_utils.get_sequencing_instrument_from_dataset(dataset)}

    if len(file_resources) == 0:
        raise Exception('No sequence datasets that match lanes {}'.format(requested_lane_ids))

    storage_id = get_storage(location)
    fastqs = {}

    for resource in file_resources:
        fastq = get_file_resource(resource)
        fastq['lane_id'] = lanes_dict[resource]['lane_id']
        fastq['sequencing_centre'] = lanes_dict[resource]['sequencing_centre']
        fastq['sequencing_instrument'] = lanes_dict[resource]['sequencing_instrument']
        if not tantalus_utils.file_resource_match_location(fastq, storage_id):
            continue
        if not tantalus_utils.file_resource_match_index_sequence(fastq, index_sequences):
            continue
        index = (tantalus_utils.get_index_sequence_from_file_resource(fastq), 
            tantalus_utils.get_read_end_from_file_resource(fastq))
        fastqs[index] = fastq

    return fastqs, sequence_datasets


def tag_datasets(datasets, tag_name):
    """ Tag a list of datasets for later reference """
    table = ['dataset', 'tag_create']
    params = {'name': tag_name, 'sequencedataset_set': list(datasets)}
    r = CLIENT.action(SCHEMA, table, params = params)


def start_file_transfer(source, destination, tag_name, transfer_name):
    """ Get an existing file transfer by name, or create a new one """

    source_id = get_storage(source)
    destination_id = get_storage(destination)

    # Check if file transfer already exists
    get_table = ['file_transfer', 'list']
    params = {'name': transfer_name}
    log.debug('Trying to get file transfer {} with table {}'.format(transfer_name, get_table))
    r = CLIENT.action(SCHEMA, get_table, params = params)

    if r['count'] != 0:
        # The file transfer instance already exists
        result = r['results'][0]
        transfer_id = result['id']

        if not result['running'] and result['finished'] and not result['success']:
            # File transfer finished but did not succeed
            raise Exception('File transfer started but did not finish. \
                Try restarting it at http://tantalus.bcgsc.ca/filetransfers/{}'.format(transfer_id))

        log.warning('File transfer {} exists. Make sure all datasets have been transferred.'.format(transfer_name))
    else:
        post_table = ['file_transfer', 'create']
        body = {"from_storage": source_id, "to_storage": destination_id, "name": transfer_name, "tag_name": tag_name}
        log.debug("Trying to create file transfer {} at table {}".format(transfer_name, post_table))
        r = CLIENT.action(SCHEMA, post_table, params = body)
        transfer_id = r['results'][0]['id']

    return transfer_id


def wait_for_file_transfer(transfer_id, transfer_name):
    """ Wait for a file transfer to finish """
    table = ['file_transfer', 'list']
    params = {'id': transfer_id}

    while True:
        log.debug('Querying file transfer {} with ID {}'.format(transfer_name, transfer_id))
        r = CLIENT.action(SCHEMA, table, params = params)
        if r['results'][0]['finished']:
            return r['results'][0]
        time.sleep(10)


def do_file_transfer(source, destination, tag_name, transfer_name):
    """ Create and wait for a file transfer

    If the file transfer is already running, wait until it is finished.
    """

    transfer_id = start_file_transfer(source, destination, tag_name, transfer_name)
    transfer_info = wait_for_file_transfer(transfer_id, transfer_name)

    # Print errors for each transfer if failed
    if not transfer_info['success']:
        message = "Transfer {} of tag {} from storage {} to {} failed with:\n{}".format(
            transfer_id, tag_name,
            source, destination, transfer_info["state"])
        log.error(message)
        raise Exception(message)


def send_brc2fastq_path_outputs_to_tantalus(outputs, storage_name):
    storage = get_storage(storage_name)
    tasks = []

    for flowcell_id, output in outputs.items():
        if output.startswith('/genesis'):
            output = output[len('/genesis'):]
        get_table = ['brc_import_fastqs', 'list']
        post_table = ['brc_import_fastqs', 'create']
        tasks.append(create_resource(get_table, post_table,
                                    {"output_dir": output, 
                                    "storage": storage, 
                                    "flowcell_id": flowcell_id}))
    wait_for_finish(get_table, tasks)


def get_bam_ids(library_id, bam_paths):
    bam_paths = set(bam_paths)
    sequence_datasets = set()
    file_resources = set()

    for dataset in get_sequence_datasets(library_id = library_id):
        if not tantalus_utils.sequence_dataset_match_type(dataset, 'BAM'):
            continue
        sequence_datasets.add(dataset['id'])
        file_resources.update(dataset['file_resources'])

    if len(file_resources) == 0:
        raise Exception('No bams for library {}'.format(library_id))

    storage_id = get_storage('singlecellblob')

    for resource in file_resources:
        bam = get_file_resource(resource)
        if not tantalus_utils.file_resource_match_location(bam, storage_id):
            continue

    return sequence_datasets


def push_bams_to_tantalus(bams, location):
    storage = get_storage(location)

    get_table = ['import_dlp_bam', 'list']
    post_table = ['import_dlp_bam', 'create']
    query_id = [create_resource(get_table, post_table, 
            {"bam_paths": bams, "storage": storage})]
    wait_for_finish(get_table, query_id)


def create_file_instance(file_resource_id, storage_id):
    get_table = ['file_instance', 'list']
    post_table = ['file_instance', 'create']
    params = {
        'file_resource_id': file_resource_id,
        'storage_id': storage_id,
    }
    create_resource(get_table, post_table, params)


def get_or_create(name, **fields):
    get_params = {}
    for field in SCHEMA[name]['list'].fields:
        if field.name in ('limit', 'offset'):
            continue
        if field.name in fields:
            get_params[field.name] = fields[field.name]

    list_results = CLIENT.action(SCHEMA, [name, 'list'], params=get_params)

    if list_results['count'] > 1:
        raise ValueError('more than 1 object for {}, {}'.format(
            name, fields))

    elif list_results['count'] == 1:
        result = list_results['results'][0]

        for field_name, field_value in fields.iteritems():
            if field_name not in result:
                raise ValueError('field {} not in {}'.format(
                    field_name, name))

            if result[field_name] != field_value:
                raise ValueError('field {} already set to {} not {}'.format(
                    field_name, result[field_name], field_value))

    else:
        result = CLIENT.action(SCHEMA, [name, 'create'], params=fields)

    return result
