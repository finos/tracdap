/**
 * A component containing a PDF document using building blocks from the react-pdf plugin.
 * This uses the plugin components to lay out blocks that have styles added to them that
 * the plugin then renders into a PDF.
 *
 * @remarks This component is not mounted in the DOM, it is called as a function in the
 * {@link ButtonToDownloadPdf}, this means that it can not access the Redux store so must
 * be passed all the required information using its props.
 *
 * @module PdfInfoDocument
 * @category Component
 */

import type {CodeRepositories, Images, Option} from "../../../types/types_general";
import {convertKeyToText} from "../../utils/utils_string";
import {convertObjectTypeToString} from "../../utils/utils_trac_metadata";
import {DataViewerPdf} from "./DataViewerPdf";
import {Document, Image, Page, StyleSheet, Text} from "@react-pdf/renderer";
import {FlowViewerPdf} from "./FlowViewerPdf";
import {JobViewerPdf} from "./JobViewerPdf";
import {ModelViewerPdf} from "./ModelViewerPdf";
import {PdfCss} from "../../../config/config_pdf_css";
import React from "react";
import PropTypes from "prop-types";
import {SchemaViewerPdf} from "./SchemaViewerPdf";
import {tracdap as trac} from "@finos/tracdap-web-api";
import type {UiAttributesProps} from "../../../types/types_attributes_and_parameters";

/**
 * An interface for the props of the PdfInfoDocument component.
 */
export interface Props {

    /**
     * The attribute data from the {@link setAttributesStore}, this is used to augment the presentation
     * of the attribute table with formats and the correct names.
     */
    allProcessedAttributes: Record<string, UiAttributesProps>
    /**
     * Information about the client image to load into the PDF.
     */
    clientImages: Images["client"]
    /**
     * The details of the configured model repositories in the UI config, this is needed so we can add information about
     * where the model is stored.
     */
    codeRepositories: CodeRepositories
    /**
     * The SVG image of the flow (if the object being reported on is a flow). This needs to be passed as a prop
     * because it can't be loaded from  the store because this component is not in the DOM, so it is not within
     * <Provider> tags to be able to access the store. The ButtonToDownloadPdf is the closest component to this
     * component, but we pass it in from the FlowViewer component.
     */
    svg?: null | React.ReactElement
    /**
     * The SVG image of the key for the flow (if the object being reported on is a flow). This needs to be passed as a
     * prop because it can't be loaded from  the store because this component is not in the DOM, so it is not within
     * <Provider> tags to be able to access the store. The ButtonToDownloadPdf is the closest component to this
     * component, but we pass it in from the FlowViewer component.
     */
    svgKey?: null | React.ReactElement
    /**
     * The metadata for the object to create a PDF report on.
     */
    tag: trac.metadata.ITag
    /**
     * The tenant option that the user is working under.
     */
    tenant: Option<string>
    /**
     * The username of the person using the application.
     */
    userName: string
    /**
     * The user ID of the person using the application.
     */
    userId: string
}

export const PdfInfoDocument = (props: Props) => {

    const {allProcessedAttributes, clientImages, codeRepositories, svg, svgKey, tag, tenant, userName, userId} = props

    // Create the css styles for the exported PDF elements
    const styles = StyleSheet.create(PdfCss)

    return (
        <Document
            title={`${convertKeyToText(convertObjectTypeToString(tag?.header?.objectType || trac.ObjectType.OBJECT_TYPE_NOT_SET))} governance report`}
            author={`${userName} (${userId})`}
            keywords={tag.header?.objectId || undefined}
            creator={"TRAC application"}
        >
            <Page size="A4"
                  style={styles.page}
            >
                {/*The image used for the web page banner needs to be 30 pixels in the pdf*/}
                <Image fixed src={clientImages.lightBackground.src} style={[styles.headerLogo, {
                    width: clientImages.lightBackground.displayWidth * 30 / clientImages.lightBackground.displayHeight,
                    height: 30
                }]}/>

                {tag && tag.hasOwnProperty("header") && tag.header?.objectType === trac.ObjectType.MODEL &&
                    <ModelViewerPdf codeRepositories={codeRepositories} allProcessedAttributes={allProcessedAttributes} tag={tag} tenant={tenant} userName={userName} userId={userId}/>
                }

                {tag && tag.hasOwnProperty("header") && tag.header?.objectType === trac.ObjectType.DATA &&
                    <DataViewerPdf allProcessedAttributes={allProcessedAttributes} tag={tag} tenant={tenant} userName={userName} userId={userId}/>
                }

                {tag && tag.hasOwnProperty("header") && tag.header?.objectType === trac.ObjectType.SCHEMA &&
                    <SchemaViewerPdf allProcessedAttributes={allProcessedAttributes} tag={tag} tenant={tenant} userName={userName} userId={userId}/>
                }

                {tag && tag.hasOwnProperty("header") && tag.header?.objectType === trac.ObjectType.JOB &&
                    <JobViewerPdf allProcessedAttributes={allProcessedAttributes} tag={tag} tenant={tenant} userName={userName} userId={userId} svg={svg}/>
                }

                {tag && tag.hasOwnProperty("header") && tag.header?.objectType === trac.ObjectType.FLOW &&
                    <FlowViewerPdf allProcessedAttributes={allProcessedAttributes} tag={tag} tenant={tenant} userName={userName} userId={userId} svg={svg} svgKey={svgKey}/>
                }

                <Text style={styles.pageNumber} render={({pageNumber, totalPages}) => (
                    `Page ${pageNumber} of ${totalPages}`
                )} fixed/>

            </Page>
        </Document>
    )
};

PdfInfoDocument.propTypes = {

    allProcessedAttributes: PropTypes.object,
    codeRepositories: PropTypes.arrayOf(PropTypes.object),
    clientImages: PropTypes.object.isRequired,
    svg: PropTypes.object,
    tag: PropTypes.object,
    tenant: PropTypes.object.isRequired,
    userName: PropTypes.string.isRequired,
    userId: PropTypes.string.isRequired
};