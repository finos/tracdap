/**
 * A component that shows the user to see a summary of a particular version of an import model job stored in TRAC.
 * This component is for viewing in a PDF, there is a sister component called {@link JobViewerImportModel} that
 * is for viewing in a browser. These two components need to be kept in sync so if a change is made to one then
 * it should be reflected in the other.
 *
 * @module JobViewerImportModelPdf
 * @category Component
 */

import {checkProperties, enrichProperties} from "../../utils/utils_trac_metadata";
import {convertKeyToText} from "../../utils/utils_string";
import {isObject, isPrimitive} from "../../utils/utils_trac_type_chckers";
import {PdfCss} from "../../../config/config_pdf_css";
import PropTypes from "prop-types";
import React from "react";
import {setPdfTableCellStyle} from "../../utils/utils_general";
import {StyleSheet, Text, View} from "@react-pdf/renderer";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiAttributesProps} from "../../../types/types_attributes_and_parameters";

// This is a list of the metadata attributes from the job that we want to show that relate to the
// model that was imported. A tag that is an array means that the property is deeply nested.
const modelPropertiesToShow: { tag: "attrs" | "header" | string[], property: string }[] = [
    {tag: ["definition", "job", "importModel"], property: "entryPoint"},
    {tag: ["definition", "job", "importModel"], property: "language"},
    {tag: ["definition", "job", "importModel"], property: "path"},
    {tag: ["definition", "job", "importModel"], property: "repository"},
    {tag: ["definition", "job", "importModel"], property: "version"}
]

// This is a list of the metadata attributes that were added to the model when the job was run. modelAttrs is an array
// of tag updates.
const modelAttrsToShow: { tag: "attrs" | "header" | string[], property: string }[] = [
    {tag: ["definition", "job", "importModel"], property: "modelAttrs"}
]

/**
 * An interface for the props of the JobViewerImportModelPdf component.
 */
export interface Props {

    /**
     * The attribute data from the {@link setAttributesStore}, this is used to augment the presentation
     * of the attribute table with formats and the correct names.
     */
    allProcessedAttributes?: Record<string, UiAttributesProps>
    /**
     * The downloaded metadata for the selected job.
     */
    job: trac.metadata.ITag
}

export const JobViewerImportModelPdf = (props: Props) => {

    const {allProcessedAttributes, job} = props

    // Create the css styles for the exported PDF elements
    const styles = StyleSheet.create(PdfCss)

    const finalModelPropertiesToShow = allProcessedAttributes ? enrichProperties(checkProperties(modelPropertiesToShow, job), job, allProcessedAttributes) : []
    const finalModelAttrsToShow = allProcessedAttributes ? enrichProperties(checkProperties(modelAttrsToShow, job), job, allProcessedAttributes) : []

    return (
        <React.Fragment>

            <View wrap={false}>
                <View style={styles.section}>
                    <Text>2. Model location</Text>
                </View>

                {finalModelPropertiesToShow.length === 0 &&
                    <Text style={styles.text}>
                        There is no information about the location of the model that was imported.
                    </Text>
                }

                {finalModelPropertiesToShow.length > 0 &&
                    <React.Fragment>
                        <Text style={styles.text}>
                            Information about the location of the model that was imported.
                        </Text>

                        <View style={styles.table}>
                            {finalModelPropertiesToShow.map((row, i) => {

                                const styleCol = setPdfTableCellStyle(styles, i, finalModelPropertiesToShow.length)

                                return (
                                    <View key={i} style={styles.tableRow}>
                                        <View style={[styleCol, {width: "33%", textAlign: "left"}]}>
                                            <Text style={styles.tableCell}>{convertKeyToText(row.key)}</Text>
                                        </View>

                                        <View style={[styleCol, {width: "67%", textAlign: "left"}]}>
                                            <Text style={styles.tableCell}>
                                                {(row.value == null) || (Array.isArray(row.value) && row.value.length === 0) || (isObject(row.value) && Object.keys(row.value).length === 0) ? "-" :
                                                    isPrimitive(row.value) ? row.value.toString() :
                                                        Array.isArray(row.value) ? row.value.join(", ") : Object.entries(row.value).map(([key, value]) => `${key}: ${value}`).join(", ")}
                                            </Text>
                                        </View>
                                    </View>
                                )
                            })}
                        </View>

                    </React.Fragment>
                }
            </View>

            <View wrap={false}>
                <View style={styles.section}>
                    <Text>3. Model metadata</Text>
                </View>

                {finalModelAttrsToShow.length === 0 &&
                    <Text style={styles.text}>
                        There is no information about the metadata that was attached to the loaded model.
                    </Text>
                }

                {finalModelAttrsToShow.length > 0 &&
                    <React.Fragment>
                        <Text style={styles.text}>
                            Information about the metadata that was attached to the loaded model.
                        </Text>

                        <View style={styles.table}>
                            {finalModelAttrsToShow.map((row, i) => {

                                const styleCol = setPdfTableCellStyle(styles, i, finalModelAttrsToShow.length)

                                return (
                                    <View key={i} style={styles.tableRow}>
                                        <View style={[styleCol, {width: "33%", textAlign: "left"}]}>
                                            <Text style={styles.tableCell}>{convertKeyToText(row.key)}</Text>
                                        </View>

                                        <View style={[styleCol, {width: "67%", textAlign: "left"}]}>
                                            <Text style={styles.tableCell}>
                                                {(row.value == null) || (Array.isArray(row.value) && row.value.length === 0) || (isObject(row.value) && Object.keys(row.value).length === 0) ? "-" :
                                                    isPrimitive(row.value) ? row.value.toString() :
                                                        Array.isArray(row.value) ? row.value.join(", ") : Object.entries(row.value).map(([key, value]) => `${key}: ${value}`).join(", ")}
                                            </Text>
                                        </View>
                                    </View>
                                )
                            })}
                        </View>
                    </React.Fragment>
                }
            </View>

        </React.Fragment>
    )
};

JobViewerImportModelPdf.propTypes = {

    allProcessedAttributes: PropTypes.object,
    job: PropTypes.object.isRequired
};