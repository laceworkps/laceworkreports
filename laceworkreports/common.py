import logging
from enum import Enum

from laceworksdk import LaceworkClient

from laceworkreports.sdk.DataHandlers import DataHandlerTypes

# Environment Variable Definitions
LACEWORK_ACCOUNT_ENVIRONMENT_VARIABLE = "LW_ACCOUNT"
LACEWORK_SUBACCOUNT_ENVIRONMENT_VARIABLE = "LW_SUBACCOUNT"
LACEWORK_API_KEY_ENVIRONMENT_VARIABLE = "LW_API_KEY"
LACEWORK_API_SECRET_ENVIRONMENT_VARIABLE = "LW_API_SECRET"
LACEWORK_API_BASE_DOMAIN_ENVIRONMENT_VARIABLE = "LW_BASE_DOMAIN"
LACEWORK_API_CONFIG_SECTION_ENVIRONMENT_VARIABLE = "LW_PROFILE"


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
    Alerts = "alerts"

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
        self.name = __name__.split(".")[0]

        # command context    self.ACTION = None
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

        # jinja context
        self.template_path = None

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

        # other
        self.other = "Default"

    def update(self, key, value):
        obj = getattr(self, key)
        obj = value
        logging.debug(f"Update {key} with {value}: self.{key} = {obj}")

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
