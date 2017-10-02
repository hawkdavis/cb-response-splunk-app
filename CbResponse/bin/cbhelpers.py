from cbapi import CbApi
from cbapi.response import CbEnterpriseResponseAPI
from cbapi.errors import ApiError, ServerError

from splunklib.searchcommands import GeneratingCommand, Option, Configuration, EventingCommand
import json
import time
import logging
import traceback
import os

import logging
logger = logging.getLogger('cbhelpers.py')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(os.environ['SPLUNK_HOME']+'/var/log/splunk/cbhelpers.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s | src="%(name)s" | lvl="%(levelname)s" | msg="%(message)s"')
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.info("CBHELPERS LOG!")


class CredentialMissingError(Exception):
    pass


try:
    from splunk.clilib.bundle_paths import make_splunkhome_path
except ImportError:
    from splunk.appserver.mrsparkle.lib.util import make_splunkhome_path


def get_creds(splunk_service):

    api_credentials = splunk_service.storage_passwords["credential:CbResponse_realm:admin:"]

    token = api_credentials.clear_password

    cb_server = splunk_service.confs["CbResponse_Settings"]["api_info"]['api_url']

    ssl_settings = splunk_service.confs['CbResponse_Settings']['api_info']['ssl_verify']

    ssl_verify = False if ssl_settings == '0' else True

    logger.debug("%s %s %s %s" % (api_credentials.clear_password, cb_server, ssl_verify, ssl_settings))

    if not cb_server or not token:
        raise CredentialMissingError(
            "Please visit the Set Up Page for the Cb Response App for Splunk to set the URL and API key for your Cb Response server.")

    return cb_server, token, ssl_verify


def get_legacy_cbapi(splunk_service):
    cb_server, token , ssl_verify= get_creds(splunk_service)
    return CbApi(cb_server, ssl_verify=ssl_verify, token=token)


def get_cbapi(splunk_service):
    cb_server, token , ssl_verify = get_creds(splunk_service)
    return CbEnterpriseResponseAPI(token=token, url=cb_server, ssl_verify=ssl_verify)


class CbSearchCommand2(EventingCommand):
    query = Option(name="query", require=False)
    max_result_rows = Option(name="maxresultrows", default=1000)

    field_names = []
    search_cls = None

    def __init__(self):
        super(CbSearchCommand2, self).__init__()
        self.setup_complete = False
        self.cb = None
        self.cb_url = "<unknown>"
        self.error_text = "<unknown>"

    def error_event(self, comment, stacktrace=None):
        error_text = {"Error": comment}
        if stacktrace is not None:
            error_text["stacktrace"] = stacktrace

        return {'sourcetype': 'bit9:carbonblack:json', '_time': time.time(), 'source': self.cb_url,
                '_raw': json.dumps(error_text)}

    def prepare(self):
        try:
            self.cb = get_cbapi(self.service)
            self.cb_url = self.cb.credentials.url
        except KeyError:
            self.error_text = "API key not set. Check that the Cb Response server is set up in the Cb Response App for Splunk configuration page."
        except (ApiError, ServerError) as e:
            self.error_text = "Could not contact Cb Response server: {0}".format(str(e))
        except CredentialMissingError as e:
            self.error_text = "Setup not complete: {0}".format(str(e))
        except Exception as e:
            self.error_text = "Unknown error reading API key from credential storage: {0}".format(str(e))
        else:
            self.setup_complete = True

    def process_data(self, data_dict):
        """
        If you want to modify the data dictionary before returning to splunk, override this. // BSJ 2016-08-30
        """
        return data_dict

    def squash_data(self, data_dict):
        for x in data_dict.keys():
            v = data_dict[x]
            data_dict[x] = str(v)
        return data_dict

    def generate_result(self, data):
        rawdata = dict( (field_name, getattr(data, field_name, "")) for field_name in self.field_names)
        squashed_data = self.squash_data( self.process_data(rawdata) )
        return {'sourcetype': 'bit9:carbonblack:json', '_time': time.time(),
                'source': self.cb_url, '_raw': squashed_data}

    def transform(self, results):
        if not self.setup_complete:
            yield self.error_event("Error: {0}".format(self.error_text))
            return                                    # explicitly stop the generator on prepare() error

        try:
            query = self.cb.select(self.search_cls)
            if self.query:
                query = query.where(self.query)

            for result in query[:int(self.max_result_rows)]:
                self.logger.info("yielding {0} {1}".format(self.search_cls.__name__, result._model_unique_id))
                yield self.generate_result(result)

        except Exception as e:
            yield self.error_event("error searching for {0} in Cb Response: {1}".format(self.query, str(e)),
                                   stacktrace=traceback.format_stack())
            return


def setup_logger():
   """
   Setup a logger for the REST handler.
   """

   logger = logging.getLogger('da-ess-cbresponse')
   logger.setLevel(logging.DEBUG)

   file_handler = logging.handlers.RotatingFileHandler(
     make_splunkhome_path(['var', 'log', 'splunk', 'da-ess-cbresponse.log']),
     maxBytes=25000000, backupCount=5)
   formatter = logging.Formatter('%(asctime)s %(lineno)d %(levelname)s %(message)s')
   file_handler.setFormatter(formatter)

   logger.addHandler(file_handler)

   return logger
