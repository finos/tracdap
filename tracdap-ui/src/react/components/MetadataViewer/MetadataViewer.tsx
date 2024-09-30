/**
 * A component that shows a series of tables corresponding to properties of the metadata of a TRAC object. This
 * component is for viewing in a browser, there is a sister component called {@link MetadataViewerPdf} that is
 * for viewing in a PDF. These two components need to be kept in sync so if a change is made to one then it
 * should be reflected in the other.
 *
 * @module MetadataViewer
 * @category Component
 */

import Col from "react-bootstrap/Col";
import {createTableData} from "../../utils/utils_attributes_and_parameters";
import {MetadataTable} from "./MetadataTable";
import {type MetadataTableToShow} from "../../../types/types_general";
import PropTypes from "prop-types";
import React, {memo} from "react";
import Row from "react-bootstrap/Row";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the MetadataViewer component.
 */
export interface Props {

    /**
     * The css class to apply to the table, this allows additional styles to be added to the component.
     * @defaultValue ''
     */
    className?: string
    /**
     * Bootstrap grid layout for the MD breakpoint.
     * @defaultValue {span: 12, offset: 0}
     */
    lgGrid?: { span: number, offset: number }
    /**
     * The TRAC metadata for an object to show in tables.
     */
    metadata: trac.metadata.ITag
    /**
     * Whether to show a title above each table.
     * @defaultValue true
     */
    showTitles?: boolean
    /**
     * The tables to break the metadata into, these need to be keys in the metadata object. The
     * user can use this to set the titles for the tables too.
     * @defaultValue [{key: "header"}, {key: "attrs", title: "Attributes"}]
     */
    tablesToShow?: MetadataTableToShow[]
    /**
     * Bootstrap grid layout for the XL breakpoint.
     * @defaultValue {span: 12, offset: 0}
     */
    xlGrid?: { span: number, offset: number }
    /**
     * Bootstrap grid layout for the XS breakpoint.
     * @defaultValue {span: 12,offset: 0}
     */
    xsGrid?: { span: number, offset: number }
}

/**
 * These are default prop values, but we don't put them directly into the destructuring inside the
 * component as that would cause re-rendering.
 */
const defaultObjectProps: Pick<Props, "lgGrid" | "tablesToShow" | "xlGrid" | "xsGrid"> = {

    lgGrid: {span: 12, offset: 0},
    tablesToShow: [{key: "header"}, {key: "attrs", title: "Attributes"}],
    xlGrid: {span: 12, offset: 0},
    xsGrid: {span: 12, offset: 0}
};

const MetadataViewerInner = (props: Props) => {

    const {
        className = "",
        metadata,
        lgGrid = defaultObjectProps.lgGrid,
        showTitles = true,
        tablesToShow = defaultObjectProps.tablesToShow,
        xlGrid = defaultObjectProps.xlGrid,
        xsGrid = defaultObjectProps.xsGrid,
    } = props;

    // Get what we need from the store
    const {allProcessedAttributes} = useAppSelector(state => state["setAttributesStore"])

    const tablesData = tablesToShow ? createTableData(metadata, tablesToShow, allProcessedAttributes) : []

    return (

        <React.Fragment>
            {tablesData.length > 0 &&
                <Row className={className}>
                    <Col xs={xsGrid} lg={lgGrid} xl={xlGrid}>
                        {tablesData.map((tableData, i) => (

                            <MetadataTable key={i} info={tableData} showTitles={showTitles}/>

                        ))}
                    </Col>
                </Row>
            }
        </React.Fragment>
    )
};

MetadataViewerInner.propTypes = {

    className: PropTypes.string,
    lgGrid: PropTypes.shape({span: PropTypes.number.isRequired, offset: PropTypes.number.isRequired}),
    metadata: PropTypes.object.isRequired,
    showTitles: PropTypes.bool,
    tablesToShow: PropTypes.arrayOf(PropTypes.shape({key: PropTypes.string.isRequired, title: PropTypes.string})),
    xlGrid: PropTypes.shape({span: PropTypes.number.isRequired, offset: PropTypes.number.isRequired}),
    xsGrid: PropTypes.shape({span: PropTypes.number.isRequired, offset: PropTypes.number.isRequired})
};

export const MetadataViewer = memo(MetadataViewerInner)