/**
 * A group of utilities for async functions.
 * @category Utils
 * @module AsyncUtils
 */

import type {ActionCreatorWithPayload} from "@reduxjs/toolkit";
import {checkForBatchMetadata, checkForMetadata} from "../store/applicationStore";
import {createUniqueObjectKey} from "./utils_trac_metadata";
import {isDefined, isObject} from "./utils_trac_type_chckers";
import {showToast} from "./utils_general";
import type {StoreStatus} from "../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch} from "../../types/types_hooks";
import {useCallback, useRef, useState} from "react";

/**
 * A function that pauses in an async action for a number of milliseconds. This is often useful if you want to visually
 * slow down a series of events in the user interface that are otherwise happening very quickly. This is not trying to
 * make the application look slow but rather there are some user work flows that need to be broken down into more easily
 * identifiable steps.
 *
 * @param milliseconds - The number of milliseconds to pause for.
 */
export function wait(milliseconds: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}

/**
 * A custom hook that gets the metadata for a tag selector using the store of metadata in the applicationStore. The
 * checkForMetadata function checks to see if the requested tag already exists in the store and if it is not there
 * it makes the request to TRAC. The hook also handles errors and an isDownloading state. ALl of this means that the
 * hook can be used as 'const [isDownloading, tag] = useMetadataStore(tagSelector, setFlowNodeOptions)' in a
 * component, the callback argument is a function dispatched to the Redux store after the metadata is returned.
 *
 * @remarks The 'suppressError' option is available because the user can in some cases trigger an API call to fetch
 * metadata without this being checked (for example entering a URL into the browser to 'data-summary/f2ac00e0-4601-42fc-99a8-14b2cb6e3654/2/1').
 * In these cases we don't want the application to error out.
 *
 * @param tagSelector - The tag selector to get the tag for.
 * @param options - An object containing optional callback functions to run when the tag is downloaded. Both dispatched and non-dispatched options are supported.
 */
export function useMetadataStore(tagSelector: null | undefined | trac.metadata.ITagSelector, options?: { suppressError?: boolean, dispatchedCallback?: ActionCreatorWithPayload<null | trac.metadata.ITag>, callback?: (payload: null | trac.metadata.ITag) => void }): [isDownloading: boolean, tag: trac.metadata.ITag | null, status: StoreStatus] {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // If fetching a particular tag errored tag will be null still, so we need a way of preventing it being reattempted each render
    const erroredTagSelector = useRef<null | trac.metadata.ITagSelector>(null)

    const getMetadata = useCallback(async (tagSelector: trac.metadata.ITagSelector): Promise<null | trac.metadata.ITag> => {

        setState(prevState => ({...prevState, isDownloading: true, status: "pending"}))

        try {

            return await dispatch(checkForMetadata(tagSelector)).unwrap()

        } catch (error) {

            // Keep a record of the failed tag request
            erroredTagSelector.current = tagSelector

            const text = {
                title: "Failed to download the metadata",
                message: "The request to download the object's metadata did not complete successfully.",
                details: typeof error === "string" ? error : isObject(error) && typeof error.message === "string" ? error.message : undefined
            }

            showToast("error", text, "useMetadataStore/rejected")

            setState({isDownloading: false, tag: null, status: "failed"})

            if (!options?.suppressError && typeof error === "string") {
                throw new Error(error)
            } else if (!options?.suppressError) {
                console.error(error)
                throw new Error("Metadata download failed")
            } else {
                return null
            }
        }

    }, [dispatch, options?.suppressError])

    // This is the state of the data held in the hook, although it is specified in a hook and not in a component it works
    // in the same way, if either variable changes a rerender is triggered.
    const [{isDownloading, status, tag}, setState] = useState<{ isDownloading: boolean, tag: null | trac.metadata.ITag, status: StoreStatus }>({isDownloading: false, tag: null, status: "idle"})

    // We have some special rules for when to trigger the async call to get the data. Rather than using useEffect we test against
    // the specific rules for whether the tag selector is for the same object.

    // !(tagSelector.latestTag === true || tagSelector.latestObject === true) is a protection against an infinite loop. If we have been asked to download the latest version of an object
    // either by tag or object, then the result of createUniqueObjectKey function is like 'MODEL-e0762625-76c3-4d17-b0aa-8f21467d28ca-vundefined-vundefined' but the value from same
    // function applied to the downloaded tag is 'MODEL-e0762625-76c3-4d17-b0aa-8f21467d28ca-v1-v1'. These will always be seen as different and any changes in state will trigger an infinite loop.
    // So we only get the latest version once (when tag is null) or when the object ID changes.
    if (!isDownloading && tagSelector != null && (tag === null || (tag.header && createUniqueObjectKey(tag.header, true) !== createUniqueObjectKey(tagSelector, true) && !((tagSelector.latestTag === true || tagSelector.latestObject === true) && tagSelector.objectId === tag.header.objectId)))) {

        // Only run if the tag requested did not error already
        if (erroredTagSelector.current === null) {
            getMetadata(tagSelector).then(newTag => {

                if (newTag != null) {
                    erroredTagSelector.current = null
                    setState({isDownloading: false, tag: newTag, status: "succeeded"})
                }

                if (options?.dispatchedCallback) {
                    dispatch(options.dispatchedCallback(newTag))
                }
                if (options?.callback) {
                    options.callback(newTag)

                }
            })
        }
    }

    return [isDownloading, tag, status]
}

/**
 * A custom hook that gets the metadata for tag selector in an array using the store of metadata in the applicationStore.
 * The checkForMetadataBatch function checks to see if the requested tag already exists in the store and if it is not there
 * it makes the request to TRAC. The hook also handles errors and an isDownloading state. ALl of this means that the
 * hook can be used as 'const [isDownloading, tag] = useMetadataStore(tagSelector, setFlowNodeOptions)' in a
 * component, the callback argument is a function dispatched to the Redux store after the metadata is returned.
 *
 * @param tagSelectors - The array of tag selectors to get the tags for.
 * @param options - An object containing optional callback functions to run when the tag is downloaded. Both dispatched and non-dispatched options are supported.
 */
export function useMetadataStoreBatch(tagSelectors: trac.metadata.ITagSelector[], options?: { dispatchedCallback?: ActionCreatorWithPayload<trac.metadata.ITag[]>, callback?: (payload: trac.metadata.ITag[]) => void }): [isDownloading: boolean, tag: trac.metadata.ITag[]] {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // If fetching a particular tag errored tag will be null still, so we need a way of preventing it being reattempted each render
    const erroredTagSelector = useRef<null | trac.metadata.ITagSelector[]>(null)

    const getMetadata = useCallback(async (tagSelectors: trac.metadata.ITagSelector[]): Promise<trac.metadata.ITag[]> => {

        setState(prevState => ({...prevState, isDownloading: true}))

        try {

            return await dispatch(checkForBatchMetadata(tagSelectors)).unwrap()

        } catch (error) {

            // Keep a record of the failed tag request
            erroredTagSelector.current = tagSelectors

            const text = {
                title: "Failed to download the metadata",
                message: "The request to download the object's metadata did not complete successfully.",
                details: typeof error === "string" ? error : isObject(error) && typeof error.message === "string" ? error.message : undefined
            }

            showToast("error", text, "useMetadataStoreBatch/rejected")

            setState({isDownloading: false, tags: []})

            if (typeof error === "string") {
                throw new Error(error)
            } else {
                console.error(error)
                throw new Error("Metadata download failed")
            }
        }

    }, [dispatch])

    // This is the state of the data held in the hook, although it is specified in a hook and not in a component it works
    // in the same way, if either variable changes a rerender is triggered.
    const [{isDownloading, tags}, setState] = useState<{ isDownloading: boolean, tags: trac.metadata.ITag[] }>({isDownloading: false, tags: []})

    // We have some special rules for when to trigger the async call to get the data. Rather than using useEffect we test against
    // the specific rules for whether the tag selector is for the same object.
    const existingStoredObjectKeys = tags.map(tag => tag?.header ? createUniqueObjectKey(tag.header, true) : undefined).filter(isDefined)

    const requestedObjectKeys = tagSelectors.map(tag => createUniqueObjectKey(tag, true))

    // First time running
    if (!isDownloading && tagSelectors.length > 0 && tags.length === 0) {

        // Only run if the tags requested did not error already
        if (erroredTagSelector.current === null) {

            getMetadata(tagSelectors).then(newTags => {

                erroredTagSelector.current = null

                setState({isDownloading: false, tags: newTags})

                if (options?.dispatchedCallback) {
                    dispatch(options.dispatchedCallback(newTags))
                }
                if (options?.callback) {
                    options.callback(newTags)
                }
            })
        }

    } else if (!isDownloading && tagSelectors.length === 0 && tags.length > 0) {

        setState({isDownloading: false, tags: []})

        if (options?.dispatchedCallback) {
            dispatch(options.dispatchedCallback([]))
        }
        if (options?.callback) {
            options.callback([])
        }

    } else if (!isDownloading && (tags.length !== tagSelectors.length || requestedObjectKeys.some(requestedObjectKey => !existingStoredObjectKeys.includes(requestedObjectKey)))) {

        // Has already run but the requested items has changed lengths or are for different items
        // Only run if the tags requested did not error already
        if (erroredTagSelector.current === null) {

            getMetadata(tagSelectors).then(newTags => {

                erroredTagSelector.current = null

                setState({isDownloading: false, tags: newTags})

                if (options?.dispatchedCallback) {
                    dispatch(options.dispatchedCallback(newTags))
                }
                if (options?.callback) {
                    options.callback(newTags)
                }
            })
        }
    }

    return [isDownloading, tags]
}
