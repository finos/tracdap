/**
 * A component that shows a button that when you click on it downloads a dataset as a CSV file.
 * If text is provided for the button then the component shows a button with an icon and text,
 * if no text is provided then just an CSV file icon is shown.
 *
 * @module ButtonToDownloadData
 * @category Component
 */

import {Button} from "./Button";
import {downloadDataAsCsvFromStream} from "../utils/utils_downloads";
import {Icon} from "./Icon";
import {isObject} from "../utils/utils_trac_type_chckers";
import PropTypes from "prop-types";
import React, {useState} from "react";
import {showToast} from "../utils/utils_general";
import type {SizeList} from "../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../types/types_hooks";

/**
 * An interface for the props of the Button component.
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
     * The object's metadata tag, this is the data that will be downloaded.
     */
    tag?: null | trac.metadata.ITag
    /**
     * The text for the button.
     */
    text?: string,
}

export const ButtonToDownloadData = (props: Props) => {

    const {className = "", loading = false, size, tag, text} = props

    const [isDownloading, setIsDownloading] = useState<boolean>(false)

    // Get what we need from the store
    const {userId} = useAppSelector(state => state["applicationStore"].login)
    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)

    /**
     * A function that makes an API call to get the data.
     */
    async function downloadData(tenant: string): Promise<void> {

        if (tag?.header) {

            try {
                // Update state
                setIsDownloading(true)

                // Stream the data to a CSV file and initiate the download
                await downloadDataAsCsvFromStream(tag, tenant, userId)

                // Update state
                setIsDownloading(false)

            } catch (err) {

                setIsDownloading(false)

                const text = {
                    title: "Failed to download the data",
                    message: "The request to download the data did not complete successfully.",
                    details: typeof err === "string" ? err : isObject(err) && typeof err.message === "string" ? err.message : undefined
                }

                showToast("error", text, "downloadData/rejected")

                if (typeof err === "string") {
                    throw new Error(err)
                } else {
                    console.error(err)
                    throw new Error("Data download failed")
                }
            }
        }
    }

    return (

        <Button ariaLabel={"Download CSV"}
                className={`min-width-px-30 ${!text ? "p-0 m-0" : ""} ${className}`}
                disabled={!tag}
                size={size}
                isDispatched={false}
                loading={loading || isDownloading}
                onClick={() => tag != null && tenant !== undefined && downloadData(tenant)}
                variant={!text ? "link" : "info"}
        >
            <Icon ariaLabel={false}
                  className={!text ? undefined : "me-2"}
                  icon={!text ? "bi-filetype-csv" : "bi-file-arrow-down"}
                  size={!text ? "2rem" : undefined}
                  tooltip={"Download data"}
            />
            {text || null}
        </Button>
    )
};

ButtonToDownloadData.propTypes = {

    className: PropTypes.string,
    loading: PropTypes.bool,
    size: PropTypes.oneOf(['lg', 'sm']),
    tag: PropTypes.object,
    text: PropTypes.string
};