#  Licensed to the Fintech Open Source Foundation (FINOS) under one or
#  more contributor license agreements. See the NOTICE file distributed
#  with this work for additional information regarding copyright ownership.
#  FINOS licenses this file to you under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with the
#  License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import tracdap.rt.api as trac
import pandas as pd
import http.client as hc
import json
import datetime as dt


class GitHubProjectDetails(trac.TracModel):

    def define_parameters(self) -> dict[str, trac.ModelParameter]:

        return {}

    def define_inputs(self) -> dict[str, trac.ModelInputSchema]:

        repo_list_schema = trac.define_input_table(
            trac.F("repo_owner", trac.STRING, "Repository owner (individual or organization)"),
            trac.F("repo_name", trac.STRING, "Repository name"))

        return { "repo_list": repo_list_schema }

    def define_outputs(self) -> dict[str, trac.ModelOutputSchema]:

        repo_details_schema = trac.define_output_table(
            trac.F("repo_owner", trac.STRING, "Repository owner (individual or organization)"),
            trac.F("repo_name", trac.STRING, "Repository name"),
            trac.F("description", trac.STRING, "Description from the repository metadata"),
            trac.F("license", trac.STRING, "Licenses associated with the repository"),
            trac.F("last_push", trac.DATETIME, "Timestamp of the last push to the repository"))

        return { "repo_details": repo_details_schema }

    def define_resources(self) -> dict[str, trac.ModelResource]:

        return { "github_api": trac.define_external_system("http", hc.HTTPSConnection) }

    def run_model(self, ctx: trac.TracContext):

        repo_list = ctx.get_pandas_table("repo_list")
        repo_details = []

        with ctx.get_external_system("github_api", hc.HTTPSConnection) as github_api:

            github_api.connect()

            for _, row in repo_list.iterrows():

                details = self.get_repo_details(github_api, row["repo_owner"], row["repo_name"])
                repo_details.append(details)

                ctx.log.info(f"{details['repo_name']}: {details['description']}")

        ctx.put_pandas_table("repo_details", pd.DataFrame(repo_details))

    @staticmethod
    def get_repo_details(github_api: hc.HTTPSConnection, repo_owner, repo_name):

        headers = {"user-agent": "TRAC Modelling Examples", "accept": "application/json"}
        repo_url = f"/repos/{repo_owner}/{repo_name}"
        github_api.request("GET", repo_url, headers=headers)

        repo_response = github_api.getresponse()

        if repo_response.status != 200:
            raise RuntimeError(f"Failed to get repo details: {repo_response.reason} ({repo_response.status})")

        repo_details = json.loads(repo_response.read())

        description = repo_details.get("description")
        license_ = repo_details["license"]["name"]
        last_push = dt.datetime.fromisoformat(repo_details["pushed_at"]).replace(tzinfo=None)

        return {
            "repo_owner": repo_owner,
            "repo_name": repo_name,
            "description": description,
            "license": license_,
            "last_push": last_push
        }


if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(GitHubProjectDetails, "config/external_systems.yaml", "config/sys_config_ext.yaml")
