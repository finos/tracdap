/**
 * A component that allows the user to create an SQL overlay statement to run on a dataset.
 *
 * @module OverlayBuilder
 * @category Component
 */

import {AddOverlay} from "./AddOverlay";
import {createOverlayEntry, type OverlayBuilderStoreState, setCurrentlyLoadedOverlay} from "./overlayBuilderStore";
import React, {useEffect} from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";
import {Overlays} from "./Overlays";
import PropTypes from "prop-types";

/**
 * An interface for the props of the OverlayBuilder component.
 */
export interface Props {

    /**
     * A unique reference to the item having overlays applied to it. In a flow this can be the output
     * node key as these are unique.
     */
    overlayKey?: string
    /**
     * The schema of the dataset that the user has selected to query.
     */
    schema: trac.metadata.IFieldSchema[]
    /**
     *  The key in the OverlayBuilderStore to use to save the data to / get the data from.
     */
    storeKey: keyof OverlayBuilderStoreState["uses"]
}

export const OverlayBuilder = (props: React.PropsWithChildren<Props>) => {

    console.log("Rendering OverlayBuilder")

    const {children, overlayKey, schema, storeKey} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A hook that runs when the page mounts that checks if there is an entry in the store
     * for the selected overlay key and if not creates one with all the default settings. This
     * causes an extra re-render on loading the OverlayBuilder component but enables us to eliminate
     * a lot of re-renders while the user is using the tool. It also sets the current part of the
     * store in use.
     */
    useEffect(() => {

        // Update the query builder store
        dispatch(setCurrentlyLoadedOverlay({storeKey, overlayKey}))
        dispatch(createOverlayEntry({storeKey, overlayKey, schema}))

    }, [dispatch, storeKey, schema, overlayKey])

    // Get what we need from the store
    const {overlayKey: currentlyLoadedOverlayKey} = useAppSelector(state => state["overlayBuilderStore"].currentlyLoaded)

    return (

        <React.Fragment>

            <AddOverlay showButton={Boolean(overlayKey != null)}>{children}</AddOverlay>

            {/* If in entry for the dataset does not yet exist don't show the interface. The useEffect hook will create one.*/}
            {overlayKey != null && currentlyLoadedOverlayKey === overlayKey &&
                <Overlays overlayKey={overlayKey} schema={schema} storeKey={storeKey}/>
            }

        </React.Fragment>
    )
};

OverlayBuilder.propTypes = {

    storeKey: PropTypes.string.isRequired
};