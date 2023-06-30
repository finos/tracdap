/**
 * A component that shows a table listing the queries used by a flow to find the input datasets or models for a flow.
 * This component is for viewing in a PDF, there is a sister component called {@link SearchQueryListTable} that is
 * for viewing in a browser. These two components need to be kept in sync so if a change is made to one then it should
 * be reflected in the other.
 *
 * @module SearchQueryListPdf
 * @category Component
 */

import {PdfCss} from "../../../config/config_pdf_css";
import PropTypes from "prop-types";
import React from "react";
import {setPdfTableCellStyle} from "../../utils/utils_general";
import {StyleSheet, Text, View} from "@react-pdf/renderer";

// Some column width css
const columnWidths: string[] = ["50%", "50%"]

/**
 * An interface for the props of the SearchQueryListPdf component.
 */
export interface Props {

    /**
     * The queries used to find the list of options for the object in TRAC. It is undefined if no nodeSearch property
     * exists for the object in the flow, in which case this component will say that the key value is used - which is
     * the default behaviour of this application.
     */
    queries: Record<string, undefined | { attrName: string, value: string }>
}

export const SearchQueryListPdf = (props: Props) => {

    const {queries} = props

    // Create the css styles for the exported PDF elements
    const styles = StyleSheet.create(PdfCss)

    return (

        <React.Fragment>

            {(!queries || Object.keys(queries).length === 0) &&
                <View style={styles.text}>
                    <Text style={styles.text}>There are no search queries</Text>
                </View>
            }

            {queries && Object.keys(queries).length > 0 &&

                <View wrap={false}>

                    <View style={styles.table}>

                        <View style={styles.tableRow}>

                            <View style={[styles.tableHead, {width: columnWidths[0]}]}>
                                <Text style={styles.tableCell}>Attribute (a)</Text>
                            </View>


                            <View style={[styles.tableHead, {width: columnWidths[1],}]}>
                                <Text style={styles.tableCell}>Value (b)</Text>
                            </View>
                        </View>
                        {Object.entries(queries).map(([key, queryObject], i) => {

                            const styleCol = setPdfTableCellStyle(styles, i, Object.keys(queries).length)

                            return (

                                <View key={key} style={styles.tableRow}>
                                    <View style={[styleCol, {width: columnWidths[0]}]}>
                                        <Text
                                            style={[styles.tableCell]}>{queryObject ? queryObject.attrName : "key"}</Text>
                                    </View>

                                    <View style={[styleCol, {width: columnWidths[1]}]}>
                                        <Text style={[styles.tableCell]}>{queryObject ? queryObject.value : key}</Text>
                                    </View>
                                </View>
                            )
                        })}

                    </View>
                    <View style={[styles.footNotes, {marginBottom: 10}]}>
                        <Text>(a) The attribute that will be searched</Text>
                        <Text>(b) The value that will be searched for</Text>
                    </View>
                </View>
            }
        </React.Fragment>
    )
};

SearchQueryListPdf.propTypes = {

    queries: PropTypes.object.isRequired
};