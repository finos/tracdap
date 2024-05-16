/**
 * A component that shows a table of key value pairs from the metadata of an object where the list
 * of properties to show can be specified as a prop. Its used when you want to show a custom set
 * of information about an object but not the full metadata record. This component is capable of
 * showing deeply nested properties in the metadata tag.
 *
 * @module ObjectDetails
 * @category Component
 */

import {Badges} from "./Badges";
import {checkProperties, enrichProperties} from "../utils/utils_trac_metadata";
import Col from "react-bootstrap/Col";
import {capitaliseString, convertKeyToText} from "../utils/utils_string";
import {CopyToClipboardLink} from "./CopyToClipboardLink";
import {HeaderTitle} from "./HeaderTitle";
import {isObject, isPrimitive} from "../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import Table from "react-bootstrap/Table";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../types/types_hooks";
import type {Variants} from "../../types/types_general";

// The default for the propertiesToShow prop. This is defined outside the component
// in order to prevent re-renders.
const defaultPropertiesToShow : Props["propertiesToShow"]= [
    {tag: "attrs", property: "name"},
    {tag: "attrs", property: "description"},
    {tag: "header", property: "objectId"},
    {tag: "header", property: "objectVersion"},
    {tag: "header", property: "objectTimestamp"},
    {tag: "attrs", property: "trac_update_user_name"}
]

/**
 * An interface for the props of the ObjectDetails component.
 */
export interface Props {

    /**
     * Whether the table should have border.
     */
    bordered: boolean
    /**
     * The css class to apply to the outer div, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * The TRAC metadata to show in table.
     */
    metadata: trac.metadata.ITag
    /**
     * The lists of properties to show  keyed by the tag property that they are stored in. This allows for
     * properties with the same name but different values in the header and attrs to be selected. The order
     * that the array is in is the order the values will be in the table.
     */
    propertiesToShow?: { tag: "attrs" | "header" | string[], property: string }[]
    /**
     * Whether the table should be striped.
     */
    striped: boolean
    /**
     * A title to add above the table.
     */
    title?: string
     /**
     * Whether to show the table as a dark table.
     */
    variant?: Variants
}

export const ObjectDetails = (props: Props) => {

    const {bordered, className = "", metadata, propertiesToShow = defaultPropertiesToShow, striped, title, variant} = props;

    // Get what we need from the store
    const {allProcessedAttributes} = useAppSelector(state => state["setAttributesStore"])

    // Work out which of the properties requested actually exist and then get the data out of the TRAC API response
    // and augment it with additional info from the attribute definitions
    const finalPropertiesToShow = enrichProperties(checkProperties(propertiesToShow, metadata), metadata, allProcessedAttributes)

    return (

        <React.Fragment>
            {finalPropertiesToShow.length === 0 &&
                "None of the information requested was found"
            }

            {finalPropertiesToShow.length > 0 &&
                <Row className={className}>
                    <Col>
                        {title &&
                            <HeaderTitle type={"h5"} text={capitaliseString(title)}/>
                        }

                        <Table bordered={bordered}
                               className={`fs-13 ${title ? "mt-3" : "mt-2"} mb-2 table-no-outline`}
                               size={"sm"}
                               striped={striped}
                               variant={variant}
                        >
                            <tbody>
                            {finalPropertiesToShow.map((row, i) =>

                                <tr key={i}>
                                    <td className={"text-nowrap align-top pe-2"}>{convertKeyToText(row.key)}:</td>

                                    <td colSpan={row.key === "objectId" ? 1 : 2}>
                                        {(row.value == null) || (Array.isArray(row.value) && row.value.length === 0) || (isObject(row.value) && Object.keys(row.value).length === 0) ? "-" :
                                            isPrimitive(row.value) ? <span
                                                    className={"user-select-all"}>{row.value.toString()}</span> :
                                                <Badges convertText={false}
                                                        text={row.value}
                                                        variant={variant === "dark" ? "secondary" : "light"}/>}
                                    </td>

                                    {row.key === "objectId" && row.value != null && isPrimitive(row.value) &&
                                        <td className={"text-end"}>
                                            <CopyToClipboardLink copyText={row.value}/>
                                        </td>
                                    }
                                </tr>
                            )}
                            </tbody>
                        </Table>

                    </Col>
                </Row>
            }
        </React.Fragment>
    )
}

ObjectDetails.propTypes = {
    bordered: PropTypes.bool.isRequired,
    className: PropTypes.string,
    metadata: PropTypes.shape({
        header: PropTypes.shape({
            objectId: PropTypes.string.isRequired,
            tagVersion: PropTypes.number.isRequired,
            objectVersion: PropTypes.number.isRequired,
            objectTimestamp: PropTypes.object.isRequired,
            tagTimestamp: PropTypes.object.isRequired,
        }).isRequired,
        attrs: PropTypes.object.isRequired,
    }).isRequired,
    propertiesToShow: PropTypes.arrayOf(PropTypes.shape(
        {
            tag: PropTypes.oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]).isRequired,
            property: PropTypes.string.isRequired
        }
    )),
    title: PropTypes.string,
    variant: PropTypes.string
};