/**
 * A component that shows a table listing of either the input or output datasets from a flow or model. This component
 * is for viewing in a browser, there is a sister component called {@link ModelDatasetListPdf} that is for viewing in
 * a PDF These two components need to be kept in sync so if a change is made to one then it should be reflected in the
 * other.
 *
 * @module ModelDatasetListTable
 * @category Component
 */

import {applyBooleanFormat} from "../utils/utils_formats";
import {convertKeyToText} from "../utils/utils_string";
import {convertPartTypeToString} from "../utils/utils_trac_metadata";
import {hasOwnProperty} from "../utils/utils_trac_type_chckers";
import {Icon} from "./Icon";
import PropTypes from "prop-types";
import React from "react";
import Table from "react-bootstrap/Table";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the ModelDatasetListTable component.
 */
export interface Props {

    /**
     * The css class to apply to the table, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The TRAC schema object for the inputs or outputs of a model object. This is keyed by the input/output key. When viewing a
     * model you get the schemas but when viewing a flow you get the flow node, we only used limited elements of these.
     */
    datasets?: null | { [key: string]: trac.metadata.IModelInputSchema | trac.metadata.IModelOutputSchema | (trac.metadata.IFlowNode & { type?: "output" | "intermediate" }) }
    /**
     * What type of dataset object has been passed.
     */
    datasetType: "input" | "output"
}

export const ModelDatasetListTable = (props: Props) => {

    const {className = "", datasets, datasetType} = props

    return (

        <React.Fragment>

            {(!datasets || Object.keys(datasets).length === 0) &&
                <div className={"mb-4"}>There are no {datasetType} datasets</div>
            }

            {datasets && Object.keys(datasets).length > 0 &&

                <Table responsive className={`dataHtmlTable w-100 ${className}`}>
                    <thead>
                    <tr>
                        <th>Key</th>
                        <th>Name</th>
                        {datasetType === "output" &&
                            <th>Type</th>
                        }
                        {/*Outputs do not have a required flag*/}
                        {datasetType === "input" &&
                            <th className={"text-center"}>Required</th>
                        }
                        <th className={"text-center"}>
                            Part type<Icon ariaLabel={"Info"}
                                           className={"ms-2"}
                                           icon={"bi-info-circle"}
                                           tooltip={"Whether the dataset is partitioned by a key"}
                        />
                        </th>
                        {/*Outputs do not have a dynamicSchema flag*/}
                        {datasetType === "input" &&
                            <th className={"text-center"}>
                                Dynamic schema<Icon ariaLabel={"Info"}
                                                    className={"ms-2"}
                                                    icon={"bi-info-circle"}
                                                    tooltip={"Whether the schema is set by the model when it runs"}/>
                            </th>
                        }
                    </tr>
                    </thead>
                    <tbody>
                    {Object.entries(datasets).map(([key, dataObject]) => {

                            // We get the label either from the flow node label property or the node attributes
                            // neither are mandatory
                            let label: string | undefined
                            if (hasOwnProperty(dataObject, "label") && typeof dataObject.label === "string") {
                                label = dataObject.label
                            } else if (hasOwnProperty(dataObject, "nodeAttrs") && Array.isArray(dataObject.nodeAttrs)){
                                const nameAttr = dataObject.nodeAttrs.find(nodeAttr => nodeAttr.attrName === "name")
                                label = typeof nameAttr?.value?.stringValue === "string" ? nameAttr?.value?.stringValue : undefined
                            }

                            return (
                                <tr key={key}>
                                    <td className={"user-select-all"}>{key}</td>
                                    <td>{label || "Not set"}</td>
                                    {datasetType === "output" &&
                                        <td>{hasOwnProperty(dataObject, "type") && typeof dataObject.type === "string" ? convertKeyToText(dataObject.type) : "Not set"}</td>
                                    }
                                    {datasetType === "input" &&
                                        <td className={"text-center"}>
                                            {/*@ts-ignore*/}
                                            {applyBooleanFormat(dataObject.schema?.required) || "Not set"}
                                        </td>
                                    }
                                    <td className={"text-center"}>
                                        {/*@ts-ignore*/}
                                        {dataObject.schema?.partType != null ? convertPartTypeToString(dataObject.schema?.partType) : "None"}
                                    </td>
                                    {datasetType === "input" &&
                                        <td className={"text-center"}>
                                            {/*@ts-ignore*/}
                                            {applyBooleanFormat(dataObject.schema?.dynamicSchema) || "Not set"}
                                        </td>
                                    }
                                </tr>
                            )
                        }
                    )}
                    </tbody>
                </Table>
            }

        </React.Fragment>
    )
};

ModelDatasetListTable.propTypes = {

    className: PropTypes.string,
    datasets: PropTypes.object,
    datasetType: PropTypes.oneOf(["input", "output"]).isRequired
};