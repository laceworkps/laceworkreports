from enum import Enum

from laceworksdk import LaceworkClient

from laceworkreports.sdk.DataHandlers import DataHandlerTypes


class ActionTypes(Enum):
    Export = "export"
    Format = "format"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class ObjectTypes(Enum):
    Entities = "entities"
    Activities = "activities"
    Queries = "queries"
    Vulnerabilities = "vulnerabilities"
    Configs = "configs"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class EntitiesTypes(Enum):
    Applications = "applications"
    CommandLines = "command_lines"
    Files = "files"
    NewFileHashes = "new_file_hashes"
    InternalIPs = "internal_ip_addresses"
    K8sPods = "k8s_pods"
    NetworkInterfaces = "network_interfaces"
    Packages = "packages"
    Processes = "processes"
    Users = "users"
    Machines = "machines"
    MachineDetails = "machine_details"
    Containers = "containers"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class ActivitiesTypes(Enum):
    ChangedFiles = "changed_files"
    DNSSummaries = "dns"
    UserLogins = "user_logins"
    Connections = "connections"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class VulnerabilitiesTypes(Enum):
    Hosts = "hosts"
    Containers = "containers"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class ConfigsTypes(Enum):
    ComplianceEvalutations = "compliance_evaluations"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class ComplianceEvaluationsTypes(Enum):
    AwsCompliance = "AwsCompliance"
    AzureCompliance = "AzureCompliance"
    GcpCompliance = "GcpCompliance"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class QueriesTypes(Enum):
    Execute = "execute"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class DBInsertTypes(Enum):
    Append = "append"
    Replace = "replace"
    Fail = "fail"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class Config:
    def __init__(self):
        # command context
        self.ACTION = None
        self.TYPE = None
        self.OBJECT = None

        # lacework client
        self.client = None

        # query context
        self.start_time = None
        self.end_time = None
        self.filters = None
        self.returns = None
        self.lql_query = None
        self.dataset = ComplianceEvaluationsTypes.AwsCompliance

        # export context
        self.format = DataHandlerTypes.CSV
        self.field_map = None
        self.dtypes = None

        # csv context
        self.file_path = "export.csv"

        # db context
        self.db_engine = None
        self.db_connection = None
        self.db_table = "export"
        self.db_if_exists = DBInsertTypes.Replace

        # format
        self.flatten_json = False
        self.append = False

        # lacework client context
        self.account = None
        self.subaccount = None
        self.api_key = None
        self.api_secret = None
        self.instance = None
        self.profile = None
        self.base_domain = None

    def connect(self):
        self.client = LaceworkClient(
            account=self.account,
            subaccount=self.subaccount,
            api_key=self.api_key,
            api_secret=self.api_secret,
            instance=self.instance,
            base_domain=self.base_domain,
            profile=self.profile,
        )

        return self.client


config = Config()
ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def main():
    pass


if __name__ == "__main__":
    main()
