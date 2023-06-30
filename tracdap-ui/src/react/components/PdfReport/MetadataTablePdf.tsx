/**
 * A component that shows a table corresponding to a property of the metadata of a TRAC object. This
 * component is for viewing in a PDF, there is a sister component called {@link MetadataTable}
 * that is for viewing in a browser. These two components need to be kept in sync so if a change is
 * made to one then it should be reflected in the other.
 *
 * @module MetadataTablePdf
 * @category Component
 */

import {capitaliseString, convertKeyToText} from "../../utils/utils_string";
import type {DataValues} from "../../../types/types_general";
import {isObject, isPrimitive} from "../../utils/utils_trac_type_chckers";
import {PdfCss} from "../../../config/config_pdf_css";
import PropTypes from "prop-types";
import React from "react";
import {setPdfTableCellStyle} from "../../utils/utils_general";
import {StyleSheet, Text, View} from "@react-pdf/renderer";

/**
 * An interface for the props of the MetadataTablePdf component.
 */
export interface Props {

    /**
     * The table data as an array of objects and the title for the table. The value can be either a primitive value
     * or and array or object of primitive values.
     */
    info: { data: { key: string, value: DataValues | (DataValues)[] | Record<string, DataValues> }[], title: string }
}

export const MetadataTablePdf = (props: Props) => {

    const {info} = props

    // Create the css styles for the exported PDF elements
    const styles = StyleSheet.create(PdfCss)

    return (
        <React.Fragment>
            {info.data.length !== 0 &&
                <React.Fragment>

                    {info.title &&
                        <View style={styles.subSection}>
                            <Text>{capitaliseString(info.title)}:</Text>
                        </View>
                    }

                    <View style={styles.table} wrap={false}>

                        {props.info.data.map((row, i) => {

                            const styleCol = setPdfTableCellStyle(styles, i, info.data.length)

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
        </React.Fragment>
    )
};

MetadataTablePdf.propTypes = {

    info: PropTypes.shape({
        data: PropTypes.arrayOf(PropTypes.shape({
            key: PropTypes.string.isRequired,
            value: PropTypes.oneOfType([PropTypes.string, PropTypes.number, PropTypes.bool, PropTypes.array, PropTypes.object]).isRequired
        })).isRequired,
        title: PropTypes.string.isRequired
    }).isRequired
};