import json
import boto3
import pprint
import configparser
from tqdm import tqdm
from datetime import datetime


class StepFunction:
    name = ''  # execution UUID
    status = ''
    start_date_time = ''
    stop_date_time = ''
    execution_arn = ''
    child_execution_arn = []  # Multiple executions spawned
    parent_execution_arn = 'N/A'
    execution_metadata = ''
    collection = 'N/A'
    provider = 'N/A'
    granule_id = 'N/A'

    def __init__(self, step_execution):
        self.name = step_execution['name']
        self.status = step_execution['status']
        self.start_date_time = step_execution['startDate']
        self.stop_date_time = step_execution['startDate'] \
            if step_execution['status'] != 'SUCCEEDED' else step_execution['stopDate']
        self.execution_arn = step_execution['executionArn']

    def __str__(self):
        return self.execution_arn

    def info(self):
        info = {'status': self.status,
                'start': self.start_date_time.isoformat(),
                'duration': self.get_duration().__str__(),
                'parent': self.parent_execution_arn,
                'collection': self.collection,
                'provider': self.provider,
                'granuleId': self.granule_id}
        return info

    def get_duration(self):
        return self.stop_date_time - self.start_date_time

    def get_children(self):
        return self.child_execution_arn

    def set_parent(self):
        if self.execution_metadata == '':
            return None
        meta = self.execution_metadata['input']
        json_dict = json.loads(meta)['payload']['meta']
        arn = json_dict['source'].replace('stateMachine', 'execution')
        self.parent_execution_arn = arn + ':' + json_dict['execution_name']

    def set_granule_id(self):
        if self.execution_metadata == '':
            return None
        if self.execution_metadata['status'] != 'SUCCEEDED':
            return None
        meta = self.execution_metadata['output']
        output = json.loads(meta)['meta']['cnmResponse']
        self.granule_id = output['identifier']

    def set_execution_metadata(self, execution_metadata):
        self.execution_metadata = execution_metadata

    def set_extra_metadata(self):
        if self.execution_metadata == '':
            return None
        lambda_input = self.execution_metadata['input']
        lambda_input_dict = json.loads(lambda_input)
        self.collection = lambda_input_dict['meta']['collection']['name']

        protocol = lambda_input_dict['meta']['provider']['protocol']
        host = lambda_input_dict['meta']['provider']['host']
        provider_path = lambda_input_dict['meta']['collection']['meta']['provider_path']

        self.provider = protocol + '://' + \
                        host + '/' + \
                        (provider_path[1:] if provider_path.startswith('/') else provider_path)


def build_relation_tree(step_client, discover_list, ingest_list) -> dict:
    """
    Builds the relation tree from merging discover list and ingest list (outputs from build_discover_ingest_objects)

    :param step_client: AWS Boto3 stepfunction client
    :param discover_list: discover list from build_discover_ingest_objects
    :param ingest_list: ingest list from build_discover_ingest_objects
    :return: a semi-file relation map (dict), lacks some metadata
    """
    relation_map = {}

    for a in tqdm(discover_list, desc='Processing discover_list'):
        relation_map[a.execution_arn] = {'child': [], 'info': a.info()}

    for b in tqdm(ingest_list, desc='Processing ingest_list'):

        execution_input = step_client.describe_execution(
            executionArn=b.parent_execution_arn
        )

        b.set_execution_metadata(execution_input)
        b.set_extra_metadata()

        if b.parent_execution_arn in relation_map:
            # relation_map already has parent info
            relation_map[b.parent_execution_arn]['child'].append(
                {b.execution_arn: b.info()}
            )
        else:
            temp_step = StepFunction(execution_input)
            temp_step.set_execution_metadata(execution_input)
            temp_step.set_extra_metadata()
            relation_map[b.parent_execution_arn] = \
                {'child': [{b.execution_arn: b.info()}],
                 'info': temp_step.info()}
    return relation_map


def generate_file(relation_map, filename_subfix) -> str:
    """
    Generic file generator

    :param relation_map: a relation map
    :param filename_subfix: the subfix of the file
    :return: filename generated
    """
    filename = f'{datetime.now().isoformat()}_result_{filename_subfix}.json'

    with open(filename, 'w') as fp:
        json.dump(relation_map, fp, indent=4)
    fp.close()

    # print(f'{filename} created!')
    return filename


def generate_report(source, filename_subfix) -> None:
    """
    Generates the final report from source (dict or JSON) of build_relation_tree

    :param source: dict or json of output of append_metadata
    :param filename_subfix: subfix for the file output (json)
    """
    # source can be file or relation_map
    if isinstance(source, str) and source.endswith('.json'):
        with open(source) as json_file:
            source = json.load(json_file)

    filename = f'{datetime.now().isoformat()}_report_{filename_subfix}.json'

    with open(filename, 'w') as fp:
        for discover_granule_step in source:
            granule_info = source[discover_granule_step]['info']
            fp.write(granule_info['start'])
            fp.write('\t')
            fp.write(granule_info['status'])
            fp.write('\t')
            fp.write(granule_info['duration'])
            fp.write('\t')
            fp.write(str(granule_info['queued_granules_count']))
            fp.write('\t')
            fp.write(granule_info['collection'])
            fp.write('\t')
            fp.write(granule_info['provider'])
            fp.write('\n')

            # has children
            if append_children:
                granule_children = source[discover_granule_step]['child']
                for child in granule_children:
                    for c in child:
                        fp.write('\t')
                        fp.write(child[c]['start'])
                        fp.write('\t')
                        fp.write(child[c]['status'])
                        fp.write('\t')
                        fp.write(child[c]['duration'])
                        fp.write('\t')
                        fp.write(child[c]['granuleId'])
                    fp.write('\n')

            # has error
            if append_fail and 'fail' in granule_info:
                fp.write('\t')
                fp.write(granule_info['fail'])
                fp.write('\n')
            fp.write('\n')
    fp.close()


def build_discover_ingest_objects(step_client) -> tuple:
    """
    build_discover_ingest_objects is main step function caller;
    mainly downloads and saves data into StepFunction objects (appending extra data when possible).

    :param step_client: AWS boto3 stepfunctions client
    :return: 2 lists: discover granules step executions and ingest step executions
    """
    list_discover_exec = step_client.list_executions(
        stateMachineArn=discover_arn,
        maxResults=discover_limit
    )

    list_exec = step_client.list_executions(
        stateMachineArn=ingest_arn,
        maxResults=ingest_limit
    )

    discovers = []
    ingests = []

    # Build discover executions and content meta data
    for execution in tqdm(list_discover_exec['executions'], desc='Building list_discover_exec'):
        step = StepFunction(execution)
        execution_input = step_client.describe_execution(
            executionArn=step.execution_arn
        )
        # get execution input to parse out parent
        step.set_execution_metadata(execution_input)
        step.set_extra_metadata()
        discovers.append(step)

    # Build ingest executions and content meta data
    for execution in tqdm(list_exec['executions'], desc='Building list_exec'):
        step = StepFunction(execution)
        execution_input = step_client.describe_execution(
            executionArn=step.execution_arn
        )
        # get execution input to parse out parent
        step.set_execution_metadata(execution_input)
        step.set_parent()
        step.set_granule_id()
        ingests.append(step)

    return discovers, ingests


def append_metadata(step_client, s3_client, source) -> dict:
    """
    Appends extra data that wasn't possible during the build_discover_ingest_objects phase
    specifically captures queued_granules_count from the output of discover granule lambda
    logs an error instead if that lambda has failed

    :param step_client: AWS Boto3 stepfunctions client
    :param s3_client: AWS Boto3 S3 client (for downloading `replace` file if needed)
    :param source: dict or json of build_relation_tree
    :return: a modified source (relation_tree)
    """
    if isinstance(source, str) and source.endswith('.json'):
        with open(source) as json_file:
            source = json.load(json_file)

    for d in tqdm(source, desc='Appending additional metadata (queued_granules_count)'):
        response = step_client.get_execution_history(
            executionArn=d
        )

        # Confirm Discover Granule Step 4 exists
        if source[d]['info']['status'] == 'RUNNING':
            source[d]['info']['queued_granules_count'] = 'N/A'
            continue  # discover granule still running; skip this one item

        if response['events'][4]['type'] == 'LambdaFunctionSucceeded':
            output = response['events'][4]['lambdaFunctionSucceededEventDetails']['output']
            output = json.loads(output)
            if 'payload' in output.keys():
                granule_count = output['meta']['collection']['meta']['discover_tf']['queued_granules_count']
            else:
                if 'replace' in output.keys():  # case when we need to download the S3 replace CMN file
                    s3_response_object = s3_client.get_object(Bucket=output['replace']['Bucket'],
                                                              Key=output['replace']['Key'])
                    object_content = s3_response_object['Body'].read()
                    s3_download = json.loads(object_content.decode('utf-8'))
                    granule_count = len(s3_download['payload']['granules'])
                else:
                    granule_count = 'REPLACE NOT FOUND'
            source[d]['info']['queued_granules_count'] = granule_count
        else:
            # issue with discover granule; saving issue
            output = response['events'][4]['lambdaFunctionFailedEventDetails']['cause']
            source[d]['info']['fail'] = output
            source[d]['info']['queued_granules_count'] = 'N/A'
    return source


config = configparser.ConfigParser()
config.read('../config.ini')

discover_arn = config.get('DEFAULT', 'discover_arn')
discover_limit = config.getint('DEFAULT', 'discover_limit')
ingest_arn = config.get('DEFAULT', 'ingest_arn')
ingest_limit = config.getint('DEFAULT', 'ingest_limit')
append_children = config.getboolean('DEFAULT', 'append_children')
append_fail = config.getboolean('DEFAULT', 'append_fail')
pp = pprint.PrettyPrinter(indent=4)

step_client = boto3.client('stepfunctions')
s3_client = boto3.client('s3')

discovers, ingests = build_discover_ingest_objects(step_client)

relation_tree = build_relation_tree(step_client, discovers, ingests)

generate_file(relation_tree, 'debug_data')

relation_tree = append_metadata(step_client, s3_client, relation_tree)

generate_file(relation_tree, 'raw_data')
generate_report(relation_tree, 'final')

