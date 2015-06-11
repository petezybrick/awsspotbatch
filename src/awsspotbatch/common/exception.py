# Copyright 2015 IPC Global (http://www.ipc-global.com) and others.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Exceptions
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

class DynamoDbPartialSaveError(Exception):
    """ """
    def __init__(self, message, table_name ):
        """

        :param message: 
        :param table_name: 

        """
        super(DynamoDbPartialSaveError, self).__init__(message)
        self.table_name = table_name


class DynamoDbPutItemMaxAttemptsExceeded(Exception):
    """ """
    def __init__(self, message, table_name ):
        """

        :param message: 
        :param table_name: 

        """
        super(DynamoDbPutItemMaxAttemptsExceeded, self).__init__(message)
        self.table_name = table_name
        
        
class DynamoDbGetItemMaxAttemptsExceeded(Exception):
    """ """
    def __init__(self, message, table_name ):
        """

        :param message: 
        :param table_name: 

        """
        super(DynamoDbGetItemMaxAttemptsExceeded, self).__init__(message)
        self.table_name = table_name