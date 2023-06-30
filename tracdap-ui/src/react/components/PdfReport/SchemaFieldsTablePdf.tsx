/**
 * A component that shows a schema in a table. This component is for viewing in a PDF, there is
 * a sister component called {@link SchemaFieldsTable} that is for viewing in a browser. These
 * two components need to be kept in sync so if a change is made to one then it should be reflected
 * in the other.
 *
 * @module SchemaFieldsTablePdf
 * @category Component
 */

import {applyBooleanFormat} from "../../utils/utils_formats";
import {convertBasicTypeToString} from "../../utils/utils_trac_metadata";
import {fillSchemaDefaultValues} from "../../utils/utils_schema";
import {PdfCss} from "../../../config/config_pdf_css";
import PropTypes from "prop-types";
import React, {useMemo} from "react";
import {setPdfTableCellStyle} from "../../utils/utils_general";
import {sortArrayBy} from "../../utils/utils_arrays";
import type {Style} from "@react-pdf/types";
import {StyleSheet, Text, View} from "@react-pdf/renderer";
import {tracdap as trac} from "@finos/tracdap-web-api";

// The number of table columns is 11, although we only show 8 as we add three to give the variable
// name and label two and three column widths
// Small Single width column - we have 1 of these so ~5%
const dynamicStyles0: Style = {width: `${100 / 22}%`, textAlign: "center"}
// Single width column - we have 4 of these so ~40%
const dynamicStyles1: Style = {width: `${2 * 100 / 22}%`, textAlign: "center"}
// Double width column - we have 1 of these so ~25%
const dynamicStyles2: Style = {width: `${2.5 * 2 * 100 / 22}%`, textAlign: "left"}
// Triple width column- we have 1 of these so ~30%
const dynamicStyles3: Style = {width: `${2 * 3 * 100 / 22}%`, textAlign: "left"}

/**
 * An interface for the props of the SchemaFieldsTablePdf component.
 */
export interface Props {

    /**
     * The TRAC schema object for the dataset or schema object.
     */
    schema?: null | trac.metadata.IFieldSchema[]
}

export const SchemaFieldsTablePdf = (props: Props) => {

    const {schema} = props

    // Create the css styles for the exported PDF elements
    const styles = StyleSheet.create(PdfCss)

    const sortedSchema = useMemo(() =>

            // The fillSchemaDefaultValues function adds in the default values for fieldOrder/categorical/businessKey
            // as these are not transmitted over the wire
            sortArrayBy((schema || []).map(item => fillSchemaDefaultValues(item)), "fieldOrder")

        , [schema])

    return (

        <React.Fragment>
            {schema && Array.isArray(schema) &&
                <React.Fragment>

                    <View style={styles.table}>
                        <View style={styles.tableRow}>
                            <View style={[styles.tableHead, {minHeight: "25px"}, dynamicStyles0]}>
                                <Text style={styles.tableCell}>#</Text>
                            </View>
                            <View style={[styles.tableHead, {minHeight: "25px"}, dynamicStyles2]}>
                                <Text style={styles.tableCell}>Field name</Text>
                            </View>
                            <View style={[styles.tableHead, {minHeight: "25px"}, dynamicStyles1]}>
                                <Text style={styles.tableCell}>Field type</Text>
                            </View>
                            <View style={[styles.tableHead, {minHeight: "25px"}, dynamicStyles3]}>
                                <Text style={styles.tableCell}>Label</Text>
                            </View>
                            <View style={[styles.tableHead, {minHeight: "25px"}, dynamicStyles1]}>
                                <Text style={styles.tableCell}>Format code</Text>
                            </View>
                            <View style={[styles.tableHead, {minHeight: "25px"}, dynamicStyles1]}>
                                <Text style={styles.tableCell}>Business key</Text>
                            </View>
                            <View style={[styles.tableHead, {minHeight: "25px"}, dynamicStyles1]}>
                                <Text style={styles.tableCell}>Categorical</Text>
                            </View>
                            <View style={[styles.tableHead, {minHeight: "25px"}, dynamicStyles1]}>
                                <Text style={styles.tableCell}>Not nullable</Text>
                            </View>
                        </View>

                        {sortedSchema.map((row, i) => {

                            const styleCol = setPdfTableCellStyle(styles, i, Object.keys(sortedSchema).length)

                            return (
                                <View style={styles.tableRow} key={i}>

                                    <View style={[styleCol, dynamicStyles0]}>
                                        <Text
                                            style={styles.tableCell}>{row.fieldOrder != null ? row.fieldOrder + 1 : "Not set "}</Text>
                                    </View>
                                    <View style={[styleCol, dynamicStyles2]}>
                                        <Text style={styles.tableCell}>{row.fieldName}</Text>
                                    </View>
                                    <View style={[styleCol, dynamicStyles1]}>
                                        <Text
                                            style={styles.tableCell}>{row.fieldType != null ? convertBasicTypeToString(row.fieldType) : "Not set"}</Text>
                                    </View>
                                    <View style={[styleCol, dynamicStyles3]}>
                                        <Text style={styles.tableCell}>{row.label || "Not set"}</Text>
                                    </View>
                                    <View style={[styleCol, dynamicStyles1]}>
                                        <Text style={styles.tableCell}>
                                            {row.fieldType && [trac.STRING, trac.BOOLEAN].includes(row.fieldType) ? "-" : !row.formatCode ? "Not set" : row.formatCode}
                                        </Text>
                                    </View>
                                    <View style={[styleCol, dynamicStyles1]}>
                                        <Text style={styles.tableCell}>{applyBooleanFormat(row.businessKey)}</Text>
                                    </View>
                                    <View style={[styleCol, dynamicStyles1]}>
                                        <Text style={styles.tableCell}>{applyBooleanFormat(row.categorical)}</Text>
                                    </View>
                                    <View style={[styleCol, dynamicStyles1]}>
                                        <Text style={styles.tableCell}>{applyBooleanFormat(row.notNull)}</Text>
                                    </View>
                                </View>
                            )
                        })}

                        {schema.length === 0 &&
                            <View style={styles.tableRow}>
                                <View style={[styles.tableCol, {width: "100%", textAlign: "center"}]}>
                                    <Text style={styles.tableCell}>There is no information to display</Text>
                                </View>
                            </View>
                        }
                    </View>
                </React.Fragment>
            }
        </React.Fragment>
    )
}

SchemaFieldsTablePdf.propTypes = {

    schema: PropTypes.arrayOf(PropTypes.shape({
        fieldName: PropTypes.string.isRequired,
        fieldType: PropTypes.number.isRequired,
        fieldLabel: PropTypes.string,
        categorical: PropTypes.bool,
        businessSegment: PropTypes.bool,
        formatCode: PropTypes.string
    }))
};