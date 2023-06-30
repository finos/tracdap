/**
 * A component that shows a button that takes the user to a particular model code file in their code repository. If
 * text is provided for the button then the component shows a button with an icon and text, if no text is provided
 * then just a python file icon is shown.
 *
 * @module ButtonToViewCode
 * @category Component
 */

import {Button} from "./Button";
import {Icon} from "./Icon";
import {matchModelToRepositoryConfigAndGetUrl} from "../utils/utils_general";
import PropTypes from "prop-types";
import React from "react";
import type {SizeList} from "../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../types/types_hooks";

/**
 * An interface for the props of the ButtonToViewCode component.
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
     * The object's metadata tag, this is the object that will be downloaded.
     */
    tag?: null | trac.metadata.ITag
    /**
     * The text for the button.
     */
    text?: string,
}

export const ButtonToViewCode = (props: Props) => {

    const {className = "", loading = false, size, tag, text} = props

    // Get what we need from the store
    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)
    const {codeRepositories} = useAppSelector(state => state["applicationStore"].clientConfig)

    // The URL for the code file in the repository
    const url = tag != null && tenant !== undefined ? matchModelToRepositoryConfigAndGetUrl(codeRepositories, tag, tenant) : undefined

    return (

        <Button ariaLabel={"Download JSON"}
                className={`min-width-px-30 ${!text ? "p-0 m-0" : ""} ${className}`}
                disabled={!tag || !url}
                href={url}
                isDispatched={false}
                loading={loading}
                size={size}
                variant={!text ? "link" : "info"}
        >
            <Icon ariaLabel={false}
                  className={!text ? undefined : "me-2"}
                  icon={!text ? "bi-filetype-py" : "bi-file-arrow-down"}
                  size={!text ? "2rem" : undefined}
            />
            {text || null}
        </Button>
    )
};

ButtonToViewCode.propTypes = {

    className: PropTypes.string,
    loading: PropTypes.bool,
    size: PropTypes.oneOf(['lg', 'sm']),
    tag: PropTypes.object,
    text: PropTypes.string
};