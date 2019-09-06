#!/usr/bin/env python3
# Sept/2019 by Renata Rocha
# Description: Written for Amazon RDS Databases (not Aurora) - creates a r/w copy ("db_name") of another existing database ("db_source"), and backs up an existing db_name.
# Licence: Attribution-Share-Alike


# First we import boto, datetime and tzutc libraries to connect to RDS and manage datetime
import boto3
import datetime
import time
from dateutil.tz import tzutc


# Define instance names, where db_name is the name of the db you're recovering production to, and backup_db_name is where you're backing up your current staging data
backup_db_name = 'staging-backup'
db_name = 'staging'
# Define source of snapshot
source_db = 'production'
# VPC Settings
db_subnet_group = 'subnet-group-name'
db_vpc_sg_id = ['sg-12345678'] 
# AWS Profile 
aws_profile = 'my-profile-name'

# Opens connection with RDS
client = boto3.client('rds') 
session = boto3.Session(profile_name=aws_profile)
rds_client = session.client('rds')

# Removes existing backup to make room for new backup

def remove_old_instance():
    removeinstance = client.delete_db_instance(
        DBInstanceIdentifier=backup_db_name,
        SkipFinalSnapshot=True
        )
    waiter = client.get_waiter('db_instance_deleted')
    waiter.wait(DBInstanceIdentifier=backup_db_name)
    print("Old database removed successfully")

# Checks if a backup instance exists, is available, and if so, removes it

def check_old_instance():
# Creates a list of instances with their statuses 
    listallinstances = client.describe_db_instances()
    instancenames = []
    for instance in listallinstances['DBInstances']:
        instanceid = instance['DBInstanceIdentifier']
        instancenames.append({'id': instance['DBInstanceIdentifier'],'status': instance['DBInstanceStatus']})

# Checks if one of them is the backup instance
    for item in instancenames:
        if backup_db_name in str(item):
            print("Old instance found, checking status")
            if "available" in str(item):
                print("Old instance available, will have to remove it, please wait...")
                print(item)
                remove_old_instance()
            else:
                print("Wait for old instance to change status then check again")
                waiter = client.get_waiter('db_instance_available')
                waiter.wait(DBInstanceIdentifier=backup_db_name)
                remove_old_instance()
        else:
            print("Not an old instance, skipping")

# Backs up staging (so you don't lose yesterday's data) by renaming it using the name you defined in backup_db_name

def rename_db():
    response = client.modify_db_instance(
         DBInstanceIdentifier=db_name,
         NewDBInstanceIdentifier=backup_db_name,
         ApplyImmediately=True)
    print("Database renamed, rebooting...")
    time.sleep(120) # It takes a long time for a renamed instance to change name so we need to be patient
    waiter = client.get_waiter('db_instance_available')
    waiter.wait(DBInstanceIdentifier=backup_db_name)

# Restores last snapshot to a new db with the same name as the one you had before

def migrate_data():
# Restores to latest point in time. 

    response = client.restore_db_instance_to_point_in_time(
    SourceDBInstanceIdentifier=source_db,
    UseLatestRestorableTime=True,
    DBSubnetGroupName=db_subnet_group,
    VpcSecurityGroupIds=db_vpc_sg_id,
    DeletionProtection=False,
    TargetDBInstanceIdentifier=db_name
)

    waiter = client.get_waiter('db_instance_available')
    waiter.wait(DBInstanceIdentifier=db_name)
    print("Database Restored!")
    

# Connects to newly restored instance and does stuff to it

def connect_to_instance():

# Obtains information to establish sql connection. Works with Postgres, unsure about others
    sqlinstance = client.describe_db_instances(
    DBInstanceIdentifier=db_name
    )
    for instance in sqlinstance['DBInstances']:
        endpoint = instance['Endpoint']
        instance_arn = instance['DBInstanceArn']
        
        print(endpoint)
        print(instance_arn)
        
    #response = client.execute_sql(
    #database=db_name,
    #dbClusterOrInstanceArn=instance_arn,
    #sqlStatements='select * from test_table'
    
check_old_instance()
rename_db()
migrate_data()
connect_to_instance()
