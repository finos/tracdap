/**
 * A component that shows a button that when you click on it downloads a PDF file. The component does not make API
 * calls to get information required. If text is provided for the button then the component shows a button with an
 * icon and text, if no text is provided then just an PDF file icon is shown.
 *
 * @module ButtonToDownloadPdf
 * @category Component
 */

import {Button} from "./Button";
import {Icon} from "./Icon";
import {isObject} from "../utils/utils_trac_type_chckers";
import type {Option, SizeList} from "../../types/types_general";
import {pdf} from '@react-pdf/renderer';
import {PdfInfoDocument} from "./PdfReport/PdfInfoDocument";
import PropTypes from "prop-types";
import React, {useState} from "react";
import {saveAs} from "file-saver";
import {setDownloadName} from "../utils/utils_downloads";
import {showToast} from "../utils/utils_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../types/types_hooks";

/**
 * An interface for the props of the ButtonToDownloadPdf component.
 */
export interface Props {

    /**
     * The css class to apply to the button, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * Whether the button should be disabled, this means that the state of the button can be
     * managed by the parent component while the tag loads from the API.
     */
    loading?: boolean
    /**
     * The bootstrap size for the button.
     */
    size?: SizeList
    /**
     * The SVG image of the flow (if the object being reported on is a flow).
     */
    svg?: null | React.ReactElement
    /**
     * The SVG image of the key for the flow (if the object being reported on is a flow).
     */
    svgKey?: null | React.ReactElement
    /**
     * The object's metadata tag, this is the object that will be downloaded.
     */
    tag?: null | trac.metadata.ITag
    /**
     * The text for the button.
     */
    text?: string,
}

export const ButtonToDownloadPdf = (props: Props) => {

    const {className = "", loading = false, size, svg, svgKey, text, tag} = props

    const [creatingPdf, setCreatingPdf] = useState<boolean>(false)

    // Get what we need from the store. Note that because the PdfInfoDocument is an argument
    // to a function from a plugin the application's stores are not accessible from inside that
    // function. This means that all store variables must be loaded up here and passed in and can
    // not be loaded using useAppSelector. Essentially the PdfInfoDocument does not appear within
    // the <Provider> tags as they are not part of render so have no access to the stores.
    const {
        login: {userId, userName},
        cookies: {"trac-tenant": tenant}
    } = useAppSelector(state => state["applicationStore"])

    const {tenantOptions} = useAppSelector(state => state["applicationStore"])

    const {codeRepositories, images: {client: clientImages}} = useAppSelector(state => state["applicationStore"].clientConfig)

    const {allProcessedAttributes} = useAppSelector(state => state["setAttributesStore"])

    // Get the option for the selected tenant, this will provide both the ID and the human-readable name
    const selectedTenantOption = tenantOptions.find(option => option.value === tenant) || {value: tenant || "unknown", label: tenant || "Unknown"}

    /**
     * A function that disables the button and generates the PDF file. Then when
     * the blob is ready a download is initiated. There is no way to un-disable
     * the button after thr saveAs completes so it will only cover the async
     * function.
     */
    const generatePdfDocument = async (tag: trac.metadata.ITag, tenant: Option<string>): Promise<void> => {

        try {
            if (tag) {
                setCreatingPdf(true)

                const blob = await pdf((
                    <PdfInfoDocument allProcessedAttributes={allProcessedAttributes}
                                     clientImages={clientImages}
                                     codeRepositories={codeRepositories}
                                     svg={svg}
                                     svgKey={svgKey}
                                     tag={tag}
                                     tenant={tenant}
                                     userId={userId}
                                     userName={userName}
                    />
                )).toBlob();

                saveAs(blob, setDownloadName(tag, "pdf", true, userId));

                setCreatingPdf(false)
            }

        } catch (err) {

            setCreatingPdf(false)

            const text = {
                title: "Failed to download the PDF",
                message: "The request to generate and download the PDF did not complete successfully.",
                details: typeof err === "string" ? err : isObject(err) && typeof err.message === "string" ? err.message : undefined
            }

            showToast("error", text, "generatePdfDocument/rejected")

            if (typeof err === "string") {
                throw new Error(err)
            } else {
                console.error(err)
                throw new Error("PDF download failed")
            }
        }
    }

    return (

        <Button ariaLabel={"Download PDF"}
                className={`min-width-px-30 ${!text ? "p-0 m-0" : ""} ${className}`}
                disabled={!tag}
                isDispatched={false}
                loading={Boolean(loading || creatingPdf)}
                onClick={async () => tag != null && tenant !== undefined && await generatePdfDocument(tag, selectedTenantOption)}
                size={size}
                variant={!text ? "link" : "info"}
        >
            <Icon ariaLabel={false}
                  className={!text ? undefined : "me-2"}
                  icon={!text ? "bi-filetype-pdf" : "bi-file-arrow-down"}
                  size={!text ? "2rem" : undefined}
            />
            {text || null}
        </Button>
    )
};

ButtonToDownloadPdf.propTypes = {

    className: PropTypes.string,
    loading: PropTypes.bool,
    size: PropTypes.oneOf(['lg', 'sm']),
    svg: PropTypes.object,
    tag: PropTypes.object,
    text: PropTypes.string
};