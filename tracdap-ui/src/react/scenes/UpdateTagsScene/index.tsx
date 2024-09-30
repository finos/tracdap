/**
 * This scene allows the user to find objects in TRAC and edit their tags.
 *
 * @module UpdateTagsScene
 * @category Scene
 */

import {Alert} from "../../components/Alert";
import {convertObjectTypeToString} from "../../utils/utils_trac_metadata";
import {FindInTrac} from "../../components/FindInTrac";
import {isDefined} from "../../utils/utils_trac_type_chckers";
import React, {useEffect} from "react";
import {SceneTitle} from "../../components/SceneTitle";
import {SetAttributes} from "../../components/SetAttributes/SetAttributes";
import {setAttributesFromTag, setObjectTypes} from "../../components/SetAttributes/setAttributesStore";
import {setTag} from "./store/updateTagsStore";
import {TextBlock} from "../../components/TextBlock";
import {UpdateTagButtons} from "./components/UpdateTagButtons";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the UpdateTagsScene index component.
 */
export interface Props {

    /**
     * The title for the scene, this is set in config_menu.tsx.
     */
    title: string
}

const UpdateTagsScene = (props: Props) => {

    console.log("Rendering UpdateTagsScene")

    const {title} = props;

    // Get what we need from the store
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)
    // Get the object tags for the rows selected in the table
    const {selectedTab} = useAppSelector(state => state["findInTracStore"].uses.updateTags)
    const tags = useAppSelector(state => state["findInTracStore"].uses.updateTags.selectedTags[selectedTab])

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get the object type of the selected object and convert it to a string e.g. "dataset"
    const objectTypeAsString = tags.length > 0 && tags[0]?.header?.objectType ? convertObjectTypeToString(tags[0].header.objectType, true, true) : undefined

    /**
     * A hook that runs whenever tag corresponding to the selected object in the table changes. It resets the values
     * of the attributes in the interface to match the selection so that they can be edited.
     */
    useEffect((): void => {

        console.log(`LOG :: Setting attributes from metadata`)

        if (tags.length > 0) {
            // When selecting tags the user can change the object type they are for. The setAttributesStore needs to know tha the
            // objects have changes, so we get them from the tags that were selected. If they have changed then the attributes are
            // re-filtered to just the new object type(s) and the values are reset to default values
            dispatch(setObjectTypes({
                storeKey: "updateTags",
                objectTypes: tags.map(tag => tag?.header?.objectType ? convertObjectTypeToString(tag.header.objectType, false) : undefined).filter(isDefined)
            }))
            // Set the attributes to be what is in the tag
            dispatch(setAttributesFromTag({tag: tags[0], storeKey: "updateTags"}))
            // Store the tag selected
            dispatch(setTag({tag: tags[0]}))
        }

    }, [dispatch, tags])

    return (

        <React.Fragment>
            <SceneTitle text={title}/>

            <TextBlock className={"mt-3 mb-2"}>
                You can use the menu below to find objects such as models or datasets that have been stored in TRAC
                and edit their the metadata tags.
            </TextBlock>

            {searchAsOf &&
                <Alert variant={"warning"}>
                    You will not be able to update tags in time travel mode.
                </Alert>
            }

            <FindInTrac storeKey={"updateTags"}/>

            {tags.length > 0 &&
                <SetAttributes show={true}
                               storeKey={"updateTags"}
                               title={`Edit ${objectTypeAsString} attributes`}
                >
                    Every {objectTypeAsString} is tagged with metadata or attributes that help users understand
                    what the item contains and helps the application handle it correctly. The editor below allows
                    you to set this {objectTypeAsString}&apos;s metadata.
                </SetAttributes>
            }

            <UpdateTagButtons/>

        </React.Fragment>
    )
};

export default UpdateTagsScene;