#
#  Copyright 2019 The Feast Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Maximum interval(secs) to wait between retries for retry function
MAX_WAIT_INTERVAL: str = "60"

AWS_LAMBDA_FEATURE_SERVER_IMAGE = "feastdev/feature-server:aws"

# feature_store.yaml environment variable name for remote feature server
FEATURE_STORE_YAML_ENV_NAME: str = "FEATURE_STORE_YAML_BASE64"

# Environment variable for registry
REGISTRY_ENV_NAME: str = "REGISTRY_BASE64"

# Environment variable for toggling usage
FEAST_USAGE = "FEAST_USAGE"

# Environment variable for the path for overwriting universal test configs
FULL_REPO_CONFIGS_MODULE_ENV_NAME: str = "FULL_REPO_CONFIGS_MODULE"

# Environment variable for overwriting FTS port
FEATURE_TRANSFORMATION_SERVER_PORT_ENV_NAME: str = "FEATURE_TRANSFORMATION_SERVER_PORT"

# Default FTS port
DEFAULT_FEATURE_TRANSFORMATION_SERVER_PORT = 6569
