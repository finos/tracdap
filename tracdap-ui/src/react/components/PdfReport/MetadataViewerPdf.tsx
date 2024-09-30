/**
 * A component that shows a series of tables corresponding to properties of the metadata of a TRAC object. This
 * component is for viewing in a PDF, there is a sister component called {@link MetadataViewer} that is for
 * viewing in a browser. These two components need to be kept in sync so if a change is made to one then it
 * should be reflected in the other.
 *
 * @module MetadataViewerPdf
 * @category Component
 */

import {createTableData} from "../../utils/utils_attributes_and_parameters";
import {MetadataTablePdf} from "./MetadataTablePdf";
import type {MetadataTableToShow} from "../../../types/types_general";
import PropTypes from "prop-types";
import React from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";
import type {UiAttributesProps} from "../../../types/types_attributes_and_parameters";

/**
 * An interface for the props of the MetadataViewerPdf component.
 */
export interface Props {

    /**
     * The attribute data from the {@link setAttributesStore}, this is used to augment the presentation
     * of the attribute table with formats and the correct names.
     */
    allProcessedAttributes: Record<string, UiAttributesProps>
    /**
     * The TRAC metadata for an object to show in tables.
     */
    metadata: trac.metadata.ITag
    /**
     * The tables to break the metadata into, these need to be keys in the metadata object. The
     * user can use this to set the titles for the tables too.
     */
    tablesToShow?: MetadataTableToShow[]
}

/**
 * These are default prop values, but we don't put them directly into the destructuring inside the
 * component as that would cause re-rendering.
 */
const defaultObjectProps: Pick<Props, "tablesToShow"> = {
    tablesToShow: [{key: "header"}, {key: "attrs", title: "Attributes"}],
};

export const MetadataViewerPdf = (props: Props) => {

    const {allProcessedAttributes, metadata, tablesToShow = defaultObjectProps.tablesToShow} = props;

    const tablesData = tablesToShow !== undefined ? createTableData(metadata, tablesToShow, allProcessedAttributes) : []

    return (

        <React.Fragment>
            {Object.keys(metadata).length > 0 && tablesData.map((tableData, i) => (

                <MetadataTablePdf key={i} info={tableData}/>

            ))}
        </React.Fragment>
    )
};

MetadataViewerPdf.propTypes = {

    allProcessedAttributes: PropTypes.object.isRequired,
    metadata: PropTypes.object.isRequired,
    tablesToShow: PropTypes.array
};