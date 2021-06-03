import boto3
import sys

from botocore.retries import bucket
from errors import *

class AwsHelper:
    _boto_clients_supported = [
        's3'
    ]

    def __init__(this, aws_access_key_id: str, aws_secret_access_key_id: str):
        this._aws_access_key_id = aws_access_key_id
        this._aws_secret_access_key = aws_secret_access_key_id

    
    def __confirmOperation(this, operation_name: str, message: str = ''):
        """
        Called to ask user to confirm the operation before performing.

        Must be called for dangerous create/delete/update operations.
        """

        print('Do you really want to perform the operation: ', operation_name)
        print(message)
        print('Enter Y to continue or N to stop execution: ', end='')
        while True:
            keyboard_input = input()
            if keyboard_input in ['Y', 'y']:
                return
            elif keyboard_input in ['N', 'n']:
                raise OperationWasNotConfirmed
            else:
                print('Your input is not correct!\nPlease, press Y to confirm or N to stop execution: ', end='')
    
    
    
    def _getBotoClient(this, clientName: str, region: str = None):
        """
        Generates boto3 client for appropriate AWS service passed by clientName parameter
        using aws_access_key_id and aws_secret_access_key passed by object constructor.
        """

        if clientName not in this._boto_clients_supported:
            raise AwsServiceClientNotSupported

        print('Get client : {}\naccess_key_id: {}\nsecret_key_id: {}'.format(
            clientName, 
            ''.join('*' * (len(this._aws_access_key_id) - 4) + this._aws_access_key_id[-4:]),
            ''.join('*' * (len(this._aws_secret_access_key) - 4) + this._aws_secret_access_key[-4:])
        ))
        clientKwargs = {
            'aws_access_key_id': this._aws_access_key_id,
            'aws_secret_access_key': this._aws_secret_access_key
        }
        if region:
            clientKwargs['region_name'] = region

        return boto3.client(
            clientName,
            **clientKwargs
        )


    # S3 operations section

    def deleteBucketObject(this, bucketName: str, key: str, versionId = 'null', region: str = '', client = None, isTopAction = True):
        """
        Delete specific object from the bucket.

        Could be used to delete specified bucket object version.
        """

        if not client:
            client = this._getBotoClient('s3', region)

        if isTopAction:
            this.__confirmOperation('Bucket object deletion', '''
            Bucket name: {}
            Object key: {}
            version id: {}
            '''.format(bucketName, key, versionId))
        
        client.delete_object(Bucket=bucketName, Key=key, VersionId=versionId)


    def getS3BucketPolicies(this, bucketName: str, region: str = '', client = None):
        """
        Gets bucket policies.
        """

        if not client:
            client = this._getBotoClient('s3', region)

        bucketPolicy = client.get_bucket_policy(Bucket = bucketName)
        return bucketPolicy

    
    def listBucketObjects(this, bucketName: str, versioned: bool, excludePrefix = [], filterPrefix = [],
                region: str = '', client = None):
        """
        Lists bucket objects.

        Objects can be filtered by prefix using `excludePrefix` and `filterPrefix` parameters.
        """

        if not client:
            client = this._getBotoClient('s3', region)

        bucketObjects = []

        if not versioned:
            response = client.list_objects(Bucket = bucketName, MaxKeys = 256)
            if 'Contents' in response:
                for obj in response['Contents']:
                    bucketObjects.append({ 'Key': obj['Key'] })
        else:
            response = client.list_object_versions(Bucket = bucketName, MaxKeys = 256)

            if 'DeleteMarkers' in response:
                for obj in response['DeleteMarkers']:
                    bucketObjects.append({ 'Key': obj['Key'], 'VersionId': obj['VersionId'] })

            if 'Versions' in response:
                for obj in response['Versions']:
                    bucketObjects.append({ 'Key': obj['Key'], 'VersionId': obj['VersionId'] })
        
        # Bucket objects validation
        bucketObjects.sort(key=lambda x: len(x['Key']))
        if len(excludePrefix):
            bucketObjects = list(filter(lambda obj: False in [obj['Key'].startswith(prefix) for prefix in excludePrefix] , bucketObjects))
        
        if len(filterPrefix):
            bucketObjects = list(filter(lambda obj: True in [obj['Key'].startswith(prefix) for prefix in filterPrefix] , bucketObjects))
        return  bucketObjects

    
    def clearBucket(this, bucketName: str, bucketObjects = None, versioned: bool = None, maxDeleteCycles: int = 5,
                excludePrefix = [], filterPrefix = [], region: str = '', client = None, isTopAction = True):
        """
        Clears bucket objects.
        
        If excludePrefix and filterPrefix are not specified - all objects will be deleted. Otherwise objects
        with prefixes from excludePrefix will be retained and objects with filterPrefix will be deleted.
        """

        if not client:
            client = this._getBotoClient('s3', region)

        if versioned == None:
            versioned = client.get_bucket_versioning(Bucket = bucketName)['Status'] == 'Enabled'

        if not bucketObjects:
            bucketObjects = this.listBucketObjects(bucketName, versioned, client=client, excludePrefix=excludePrefix,
                filterPrefix=filterPrefix)

        if isTopAction:
            this.__confirmOperation('Bucket {} clear'.format(bucketName), '''
            Versioned: {}
            Objects inside: {}
            '''.format(versioned, len(bucketObjects)))

        print('\n==============================\nDelete operation confirmed. Deleting bucket objects\n==============================')
        deleteCyclesCounter = 0
        while len(bucketObjects) != 0:
            for objectToDelete in bucketObjects:
                if 'VersionId' in objectToDelete:
                    this.deleteBucketObject(bucketName, objectToDelete['Key'], versionId=objectToDelete['VersionId'], client=client, isTopAction=False)
                else:
                    this.deleteBucketObject(bucketName, objectToDelete['Key'], client=client, isTopAction=False)

            print('Bucket objects delete cycle was ended. Checking for existing objects...')
            bucketObjects = this.listBucketObjects(bucketName, versioned, client=client, excludePrefix=excludePrefix,
                filterPrefix=filterPrefix)
            if len(bucketObjects) != 0:
                deleteCyclesCounter += 1
                if deleteCyclesCounter == maxDeleteCycles:
                    raise DeleteCyclesLimitReached()
                else:
                    print('Here are more {} objects inside the bucket. Trying to delete them')
        
        print('All bucket objects were deleted successfully!')

        


    def deleteBucket(this, bucketName: str, versioned: bool = None, maxDeleteCycles: int = 5,
                region: str = '', client = None, isTopAction = True):
        """
        Deletes bucket with all objects inside.
        """

        if not client:
            client = this._getBotoClient('s3', region)

        if versioned == None:
            versioned = client.get_bucket_versioning(Bucket = bucketName)['Status'] == 'Enabled'


        bucketObjects = this.listBucketObjects(bucketName, versioned, client=client)

        if isTopAction:
            this.__confirmOperation('Bucket {} deletion'.format(bucketName), '''
            Versioned: {}
            Objects inside: {}
            '''.format(versioned, len(bucketObjects)))

        this.clearBucket(bucketName, bucketObjects, versioned, maxDeleteCycles, region, client, False)

        print('Starting bucket delete operation.')
        print(client.delete_bucket(Bucket=bucketName))


# To run this code you need to provide 2 positional command-line arguments:
# aws_access_key_id and aws_secret_access_key from your aws-account.
if __name__ == "__main__":
    # AwsHelper object initialization
    if len(sys.argv) != 3:
        raise Exception('Bad arguments! Please, execute script with template: python awshelper.py <aws_access_key_id> <aws_secret_access_key>')
    aws_access_key_id= sys.argv[1]
    aws_secret_access_key= sys.argv[2]

    awsHelper = AwsHelper(aws_access_key_id=aws_access_key_id, aws_secret_access_key_id=aws_secret_access_key)


    # Usage examples:
    ## bucketName = 'your bucket name'

    ## Delete bucket with all objects inside
    ## awsHelper.deleteBucket(bucketName)

    ## Clear bucket
    ## awsHelper.clearBucket(bucketName)

    ## Delete object from bucket
    ## awsHelper.deleteBucketObject(bucketName, 'enter_object_key', versionId = 'enter_version_id')

    ## List bucket objects
    ## print(awsHelper.listBucketObjects(bucketName, True))

    ## Get bucket policies
    ## print(awsHelper.getS3BucketPolicies(bucketName))
