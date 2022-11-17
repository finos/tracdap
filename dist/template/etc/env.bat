@rem Copyright 2022 Accenture Global Solutions Limited
@rem
@rem Licensed under the Apache License, Version 2.0 (the "License");
@rem you may not use this file except in compliance with the License.
@rem You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

@rem You can Use this file to override environment variables in Linux / macOS deployments
@rem In some settings this is more convenient that passing variables to the control script

@rem Use "set" to set variables, e.g.

@rem set CONFIG_FILE=trac-platform-custom.yaml
@rem set CONFIG_FILE=D:\trac\config\trac-platform.yaml

@rem These are some variables that might useful:

@rem CONFIG_FILE - Location of the primary config file (relative paths are relative to %APP_HOME%\\etc)
@rem LOG_DIR - Log directory (only applies to file-based logging)
@rem PLUGINS_DIR - Location for standard plugins, JARs in this folder will be added to the classpath
@rem PLUGINS_ENABLED - Whether to load plugins from PLUGIN_DIR, defaults to true
@rem PLUGINS_EXT_DIR - Location for external plugins, JARs in this folder will be added to the classpath
@rem PLUGINS_EXT_ENABLED - Whether to load plugins from PLUGIN_EXT_DIR, defaults to false
@rem SECRET_KEY - Location of the JKS secret key

@rem JAVA_HOME - Use a specific JVM
@rem JAVA_OPTS - Java options passed to the JVM on startup

