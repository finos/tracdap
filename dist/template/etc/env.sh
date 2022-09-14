# Copyright 2022 Accenture Global Solutions Limited
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

# You can Use this file to override environment variables in Linux / macOS deployments
# In some settings this is more convenient that passing variables to the control script

# These are some variables that might useful:

# CONFIG_FILE - Location of the primary config file
# LOG_DIR - Log directory (only applies to file-based logging)
# PLUGINS_DIR - Location for standard plugins, JARs in this folder will be added to the classpath
# PLUGINS_ENABLED - Whether to load plugins from PLUGIN_DIR, defaults to true
# PLUGINS_EXT_DIR - Location for external plugins, JARs in this folder will be added to the classpath
# PLUGINS_EXT_ENABLED - Whether to load plugins from PLUGIN_EXT_DIR, defaults to false

# JAVA_HOME - Use a specific JVM
# JAVA_OPTS - Java options passed to the JVM on startup
