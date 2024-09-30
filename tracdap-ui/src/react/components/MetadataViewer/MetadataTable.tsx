/**
 * A component that shows a table corresponding to a property of the metadata of a TRAC object. This
 * component is for viewing in a browser, there is a sister component called {@link MetadataTablePdf}
 * that is for viewing in a PDF. These two components need to be kept in sync so if a change is made
 * to one then it should be reflected in the other.
 *
 * @module MetadataTable
 * @category Component
 */

import {Badges} from "../Badges";
import {capitaliseString, convertKeyToText} from "../../utils/utils_string";
import {CopyToClipboardLink} from "../CopyToClipboardLink";
import {type DataValues} from "../../../types/types_general";
import {HeaderTitle} from "../HeaderTitle";
import {isObject, isPrimitive} from "../../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React from "react";
import Table from "react-bootstrap/Table";

/**
 * An interface for the props of the MetadataTable component.
 */
export interface Props {

    /**
     * The table data as an array of objects and the title for the table. The value can be either a primitive value
     * or and array or object of primitive values.
     */
    info: { data: { key: string, value: DataValues | (DataValues)[] | Record<string, DataValues>}[], title: string }
    /**
     * Whether to show a title above the table.
     */
    showTitles: boolean
}

export const MetadataTable = (props: Props) => (

    <React.Fragment>
        {props.info.data.length !== 0 &&
            <React.Fragment>
                {props.showTitles && props.info.title &&
                    <HeaderTitle type={"h5"} text={capitaliseString(props.info.title)}/>
                }

                <Table size={"sm"} striped responsive className={"mt-2"}>
                    <tbody>

                    {props.info.data.map((row) => (
                        <tr key={row.key}>
                            <td className={"text-nowrap align-top"}>{convertKeyToText(row.key)}</td>
                            <td colSpan={row.key === "objectId" ? 1 : 2}>
                                {/*This handles arrays and objects*/}
                                {(row.value == null) || (Array.isArray(row.value) && row.value.length === 0) || (isObject(row.value) && Object.keys(row.value).length === 0) ? "-" :
                                    isPrimitive(row.value) ? <span
                                            className={"user-select-all"}>{row.value.toString()}</span> :
                                        <Badges convertText={false}
                                                text={row.value}
                                        />
                                }
                            </td>
                            {row.key === "objectId" && row.value != null && isPrimitive(row.value) &&
                                <td className={"text-end"}>
                                    <CopyToClipboardLink copyText={row.value}/>
                                </td>
                            }
                        </tr>
                    ))}

                    </tbody>
                </Table>
            </React.Fragment>
        }

    </React.Fragment>
);

MetadataTable.propTypes = {

    info: PropTypes.shape({
        data: PropTypes.arrayOf(PropTypes.shape({
            key: PropTypes.string.isRequired,
            value: PropTypes.oneOfType([PropTypes.string, PropTypes.number, PropTypes.bool, PropTypes.array, PropTypes.object]).isRequired
        })).isRequired,
        title: PropTypes.string.isRequired
    }).isRequired,
    showTitles: PropTypes.bool.isRequired
};