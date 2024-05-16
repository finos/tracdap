/**
 * A component that takes a tag selector for an object in TRAC and checks if it is the latest version. In either
 * case a message can be shown to the user about the status of the object. This is useful when the user is viewing
 * an object, and you want to alert then that although this version was used somewhere that a new version is
 * available.
 *
 * @module IsLatestVersion
 * @category Component
 */

import {Alert} from "./Alert";
import {convertObjectTypeToString} from "../utils/utils_trac_metadata";
import PropTypes from "prop-types";
import React from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../types/types_hooks";
import {useMetadataStore} from "../utils/utils_async";

/**
 * An interface for the props of the IsLatestVersion component.
 */
export interface Props {

    /**
     * The css class to apply to the alert, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * Whether to show a success message when the provided tag is still for the latest version.
     */
    showMessageWhenIsLatest: boolean
    /**
     * The object's selector tag. The ID as well as the tag and object version are needed to
     * be able to check the version.
     */
    tagSelector?: null | trac.metadata.ITagSelector
}

export const IsLatestVersion = (props: Props) => {

    const {className, showMessageWhenIsLatest, tagSelector} = props;

    // Get what we need from the store
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)

    // The tag to get, note we remove the version properties and add the latest flags.
    const latestTagSelector = {
        objectId: tagSelector?.objectId,
        objectType: tagSelector?.objectType,
        latestObject: true,
        latestTag: true,
        objectAsOf: searchAsOf,
        tagAsOf: searchAsOf
    }

    // This is a custom hook which I am very proud of, it basically takes all the logic for getting a Tag for an object in TRAC along with
    // the error handling and whether the tag is being downloaded into a hook just like useState, it also has logic to make sure that if
    // the tag has not changed the data is not fetched again. There is a callback function that is passed which runs whenever the tag is
    // updated
    const [, latestTag] = useMetadataStore(latestTagSelector)

    return (

        <React.Fragment>
            {latestTag?.header?.objectType && (latestTag?.header?.objectVersion !== tagSelector?.objectVersion || latestTag?.header?.tagVersion !== tagSelector?.tagVersion) &&
                <Alert className={className}
                       showBullets={false}
                       variant={"warning"}
                >
                    {latestTag?.header?.objectVersion !== tagSelector?.objectVersion ? `This ${convertObjectTypeToString(latestTag.header.objectType)} is not the latest version, the latest version is ${latestTag?.header?.objectVersion}` : ""}
                    {latestTag?.header?.tagVersion !== tagSelector?.tagVersion ? `The tag information is ${latestTag?.header?.objectVersion !== tagSelector?.objectVersion ? "also" : ""} not the latest version for this ${convertObjectTypeToString(latestTag.header.objectType)}, the latest tag is version ${latestTag?.header?.tagVersion}` : ""}
                </Alert>
            }

            {showMessageWhenIsLatest && latestTag?.header?.objectType && latestTag?.header?.objectVersion === tagSelector?.objectVersion && latestTag?.header?.tagVersion === tagSelector?.tagVersion &&
                <Alert className={className}
                       showBullets={false}
                       variant={"success"}
                >
                    This is the latest version of this {convertObjectTypeToString(latestTag.header.objectType)} and the latest tag information
                </Alert>
            }

        </React.Fragment>
    )
}

IsLatestVersion.propTypes = {

    className: PropTypes.string,
    showMessageWhenIsLatest: PropTypes.bool.isRequired,
    tagSelector: PropTypes.shape({
        objectType: PropTypes.number.isRequired,
        objectId: PropTypes.string.isRequired,
        tagVersion: PropTypes.number.isRequired,
        objectVersion: PropTypes.number.isRequired
    })
};