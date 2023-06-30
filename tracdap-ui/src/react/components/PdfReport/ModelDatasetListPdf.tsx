/**
 * A component that shows a table listing of either the input or output datasets from a flow or model. This component
 * is for viewing in a PDF, there is a sister component called {@link ModelDatasetListTable} that is for viewing in a
 * browser. These two components need to be kept in sync so if a change is made to one then it should be reflected in
 * the other.
 *
 * @module ModelDatasetListPdf
 * @category Component
 */

import {setPdfTableCellStyle} from "../../utils/utils_general";
import {PdfCss} from "../../../config/config_pdf_css";
import PropTypes from "prop-types";
import React from "react";
import {Text, View, StyleSheet} from "@react-pdf/renderer";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {applyBooleanFormat} from "../../utils/utils_formats";
import {convertPartTypeToString} from "../../utils/utils_trac_metadata";
import {hasOwnProperty} from "../../utils/utils_trac_type_chckers";

// Some column width css based on what columns are needed to be shown
const columnWidths: { [key in Props["datasetType"]]: string[] } = {
    "input": ["30%", "31%", "13%", "13%", "13%"], "output": ["41%", "41%", "0%", "18%", "0%"], "intermediate": ["41%", "41%", "0%", "18%", "0%"]
}

/**
 * An interface for the props of the MetadataViewerPdf component.
 */
export interface Props {

    /**
     * The TRAC schema object for the inputs or outputs of a model object. This is keyed by the input/output key. When viewing a
     * model you get the schemas but when viewing a flow you get the flow node, we only used limited elements of these.
     */
    datasets?: null | { [key: string]: trac.metadata.IModelInputSchema | trac.metadata.IModelOutputSchema | (trac.metadata.IFlowNode & { type?: "output" | "intermediate" }) }
    /**
     * What type of dataset object has been passed.
     */
    datasetType: "input" | "output" | "intermediate"
    /**
     * Whether to include the standard text for the section.
     * @defaultValue false
     */
    noText?: boolean
    /**
     * The type of report being generated, this is used in some text.
     */
    objectType: "flow" | "model"
}

export const ModelDatasetListPdf = (props: Props) => {

    const {datasets, datasetType, noText = false, objectType} = props

    // Create the css styles for the exported PDF elements
    const styles = StyleSheet.create(PdfCss)

    // Needed because the 'Dynamic schema' column header breaks onto two lines
    const minHeaderHeight = datasetType === "input" ? "25px" : undefined

    return (

        <React.Fragment>
            {(!datasets || Object.keys(datasets).length === 0) &&
                <View style={styles.text}>
                    <Text style={styles.text}>{`There are no ${datasetType} datasets`}</Text>
                </View>
            }

            {datasets && Object.keys(datasets).length > 0 &&
                <React.Fragment>
                    {!noText &&
                        <Text style={styles.text}>
                            {`The table below shows the names of all ${datasetType} datasets for the ${objectType}, this includes ${datasetType === "input" ? "whether the dataset is required or optional," : ""} how the dataset is partitioned and whether the columns are fixed or can be dynamic. The schemas for these datasets are shown in the appendix.`}
                        </Text>
                    }
                    <View style={styles.table}>

                        <View style={styles.tableRow}>
                            <View style={[styles.tableHead, {
                                minHeight: minHeaderHeight,
                                width: columnWidths[datasetType][0]
                            }]}>
                                <Text style={styles.tableCell}>Key</Text>
                            </View>

                            <View style={[styles.tableHead, {
                                minHeight: minHeaderHeight,
                                width: columnWidths[datasetType][1]
                            }]}>
                                <Text style={styles.tableCell}>Name</Text>
                            </View>

                            {datasetType === "input" &&
                                <View
                                    style={[styles.tableHead, {
                                        minHeight: minHeaderHeight,
                                        width: columnWidths[datasetType][2],
                                        textAlign: "center"
                                    }]}>
                                    <Text style={styles.tableCell}>Required</Text>
                                </View>
                            }

                            <View style={[styles.tableHead, {
                                minHeight: minHeaderHeight,
                                width: columnWidths[datasetType][3],
                                textAlign: "center"
                            }]}>
                                <Text style={styles.tableCell}>{`Part type${noText ? "" : " (a)"}`}</Text>
                            </View>

                            {datasetType === "input" &&
                                <View
                                    style={[styles.tableHead, {
                                        minHeight: minHeaderHeight,
                                        width: columnWidths[datasetType][4],
                                        textAlign: "center"
                                    }]}>
                                    <Text style={styles.tableCell}>{`Dynamic schema${noText ? "" : " (b)"}`}</Text>
                                </View>
                            }
                        </View>
                        {Object.entries(datasets).map(([key, dataObject], i) => {

                            const styleCol = setPdfTableCellStyle(styles, i, Object.keys(datasets).length)

                            return (

                                <View key={key} style={styles.tableRow}>
                                    <View style={[styleCol, {width: columnWidths[datasetType][0]}]}>
                                        <Text style={[styles.tableCell]}>{key}</Text>
                                    </View>

                                    <View style={[styleCol, {width: columnWidths[datasetType][1]}]}>

                                        <Text style={[styles.tableCell]}>{hasOwnProperty(dataObject, "label") && dataObject.label != null ? dataObject.label : "Not set"}</Text>
                                    </View>

                                    {datasetType === "input" &&
                                        <View style={[styleCol, {
                                            width: columnWidths[datasetType][2],
                                            textAlign: "center"
                                        }]}>
                                            <Text style={[styles.tableCell]}>
                                                {/*@ts-ignore*/}
                                                {applyBooleanFormat(dataObject.schema?.required) || "Not set"}
                                            </Text>
                                        </View>
                                    }
                                    <View
                                        style={[styleCol, {width: columnWidths[datasetType][3], textAlign: "center"}]}>
                                        <Text style={[styles.tableCell]}>
                                            {/*@ts-ignore*/}
                                            {dataObject?.schema?.partType != null ? convertPartTypeToString(dataObject?.schema?.partType) : "None"}
                                        </Text>
                                    </View>

                                    {datasetType === "input" &&
                                        <View style={[styleCol, {
                                            width: columnWidths[datasetType][4],
                                            textAlign: "center"
                                        }]}>
                                            <Text style={[styles.tableCell]}>
                                                {/*@ts-ignore*/}
                                                {applyBooleanFormat(dataObject.schema?.dynamicSchema) || "Not set"}</Text>
                                        </View>
                                    }
                                </View>
                            )
                        })}

                    </View>
                    {!noText &&
                        <View>
                            <View style={styles.footNotes}>
                                <Text>(a) Whether the dataset is partitioned by a key</Text>
                                {datasetType === "input" &&
                                    <View style={styles.footNotes}>
                                        <Text>(b) Whether the schema is set by the model when it runs</Text>
                                    </View>
                                }
                            </View>
                        </View>

                    }
                </React.Fragment>
            }
        </React.Fragment>
    )
};

ModelDatasetListPdf.propTypes = {

    datasets: PropTypes.object,
    datasetType: PropTypes.oneOf(["input", "output", "intermediate"]).isRequired,
    noText: PropTypes.bool,
    objectType: PropTypes.oneOf(["model", "flow"]).isRequired
};
