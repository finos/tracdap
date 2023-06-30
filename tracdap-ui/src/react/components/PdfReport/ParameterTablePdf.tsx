/**
 * A component that shows a title, description and information about parameters from a model's metadata definition.
 * This component is for viewing in a PDF, there is a sister component called {@link ParameterTable} that is for
 * viewing in a browser. These two components need to be kept in sync so if a change is made to one then it should
 * be reflected in the other.
 *
 * @module ParameterTablePdf
 * @category Component
 */

import {convertBasicTypeToString, extractValueFromTracValueObject} from "../../utils/utils_trac_metadata";
import {isPrimitive} from "../../utils/utils_trac_type_chckers";
import {PdfCss} from "../../../config/config_pdf_css";
import PropTypes from "prop-types";
import React from "react";
import {setPdfTableCellStyle} from "../../utils/utils_general";
import {StyleSheet, Text, View} from "@react-pdf/renderer";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the ParameterTablePdf component.
 */
export interface Props {

    /**
     * Whether to include the standard text for the section.
     * @defaultValue false
     */
    noText?: boolean
    /**
     * The type of report being generated, this is used in some text.
     */
    objectType: "flow" | "model"
    /**
     * The params object of a TRAC model. This is keyed by the parameter key.
     */
    params?: null | Record<string, trac.metadata.IModelParameter>
}

export const ParameterTablePdf = (props: Props) => {

    const {params, objectType, noText = false} = props

    // Create the css styles for the exported PDF elements
    const styles = StyleSheet.create(PdfCss)

    return (

        <React.Fragment>

            {(!params || Object.keys(params).length === 0) &&
                <View style={styles.text}>
                    <Text style={styles.text}>There are no parameters</Text>
                </View>
            }

            {params && Object.keys(params).length > 0 &&
                <React.Fragment>
                    {!noText &&
                        <Text style={styles.text}>
                            The table below shows the name and type of parameters used by the {objectType}, these need
                            to set by the user in order for it to be run.
                        </Text>
                    }

                    <View style={styles.table}>
                        <View style={styles.tableRow}>
                            <View style={[styles.tableHead, {width: "35%"}]}>
                                <Text style={styles.tableCell}>Key</Text>
                            </View>
                            <View style={[styles.tableHead, {width: "15%", textAlign: "center"}]}>
                                <Text style={styles.tableCell}>Type</Text>
                            </View>
                            <View style={[styles.tableHead, {width: "35%"}]}>
                                <Text style={styles.tableCell}>Name</Text>
                            </View>
                            <View style={[styles.tableHead, {width: "15%", textAlign: "center"}]}>
                                <Text style={styles.tableCell}>Default value</Text>
                            </View>
                        </View>

                        {Object.entries(params).map(([key, paramObject], i) => {

                            const styleCol = setPdfTableCellStyle(styles, i, Object.keys(params).length)
                            const values = extractValueFromTracValueObject(paramObject.defaultValue)
                            const basicType = paramObject && paramObject?.paramType?.basicType ? convertBasicTypeToString(paramObject.paramType?.basicType, true) : "Not set"

                            return (
                                <View key={key} style={styles.tableRow}>
                                    <View style={[styleCol, {width: "35%"}]}>
                                        <Text style={[styles.tableCell]}>{key}</Text>
                                    </View>
                                    <View style={[styleCol, {width: "15%", textAlign: "center"}]}>
                                        <Text style={[styles.tableCell]}>{basicType}</Text>
                                    </View>
                                    <View style={[styleCol, {width: "35%"}]}>
                                        <Text style={[styles.tableCell]}>{paramObject.label || "Not set"}</Text>
                                    </View>
                                    <View style={[styleCol, {width: "15%", textAlign: "center"}]}>
                                        <Text style={[styles.tableCell]}>
                                            {/*This handles arrays and objects*/}
                                            {values.value === null ? "Not set" :
                                                isPrimitive(values.value) ? values.value.toString() :
                                                    Array.isArray(values.value) ? values.value.join(", ") : Object.entries(values.value).map(([key, value]) => `${key}: ${value}`).join(", ")}
                                        </Text>
                                    </View>
                                </View>
                            )
                        })}
                    </View>
                </React.Fragment>
            }

        </React.Fragment>
    )
};

ParameterTablePdf.propTypes = {

    noText: PropTypes.bool,
    objectType: PropTypes.oneOf(["model", "flow"]).isRequired,
    params: PropTypes.object
};