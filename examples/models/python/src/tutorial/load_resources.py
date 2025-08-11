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

import typing as _tp

import tracdap.rt.api as trac
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.image as img

import tutorial.schemas as schemas
import tutorial.resources as resources


class ResourcesExample(trac.TracModel):

    def define_parameters(self) -> _tp.Dict[str, trac.ModelParameter]:

        return trac.define_parameters()

    def define_inputs(self) -> _tp.Dict[str, trac.ModelInputSchema]:

        quarterly_sales_schema = trac.load_schema(schemas, "profit_by_region.csv")
        quarterly_sales = trac.define_input(quarterly_sales_schema, label="Quarterly sales data")

        return { "quarterly_sales": quarterly_sales }

    def define_outputs(self) -> _tp.Dict[str, trac.ModelOutputSchema]:

        sales_report = trac.define_output(trac.CommonFileTypes.SVG, label="Quarterly sales report")

        return { "sales_report": sales_report }

    def run_model(self, ctx: trac.TracContext):

        matplotlib.use("agg")

        quarterly_sales = ctx.get_pandas_table("quarterly_sales")

        regions = quarterly_sales["region"]
        values = quarterly_sales["gross_profit"]

        fig, (ax_img, ax_plot) = plt.subplots(
            2, 1, figsize=(8, 10),
            gridspec_kw={'height_ratios': [1, 3]})

        with trac.load_resource_stream(resources, "tracdap_banner_logo.png") as logo_stream:

            logo = img.imread(logo_stream)

            # Top panel for image
            ax_img.imshow(logo)
            ax_img.axis('off')

        ax_plot.bar(regions, values)
        ax_plot.set_title("Profit by region report")
        ax_plot.set_xlabel("Region")
        ax_plot.set_ylabel("Gross profit")

        plt.tight_layout()

        with ctx.put_file_stream("sales_report") as sales_report:
            plt.savefig(sales_report, format='svg')

        plt.close(fig)

if __name__ == "__main__":
    import tracdap.rt.launch as launch
    launch.launch_model(ResourcesExample, "config/load_resources.yaml", "config/sys_config.yaml")
