/**
 * This slice acts as the store for the {@link UpdateTagsScene}.
 *
 * @module updateTagsStore
 * @category Redux store
 */

import {areSelectValuesEqual, createTagsFromAttributes} from "../../../utils/utils_attributes_and_parameters";
import {checkForMetadata} from "../../../store/applicationStore";
import {convertTagAttributesToSelectValues} from "../../../utils/utils_trac_metadata";
import {createAsyncThunk, createSlice} from '@reduxjs/toolkit';
import {getLatestTag, updateTag} from "../../../utils/utils_trac_api";
import type {PayloadAction} from '@reduxjs/toolkit';
import type {StoreStatus} from "../../../../types/types_general";
import type {RootState} from "../../../../storeController";
import {showToast} from "../../../utils/utils_general";
import {toast} from "react-toastify";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the updateTagsStoreState Redux store.
 */
export interface UpdateTagsStoreState {

    // Whether the attributes for the particular use case have been stored. This is updated when the UI
    // downloads the attribute dataset from TRAC
    status: StoreStatus
    // A message associated with the status
    message: undefined | string
    // The original tag of the object being edited
    tag: null | trac.metadata.ITag
}

// This is the initial state of the store. Note attributes are handled in the setAttributesStore.
const initialState: UpdateTagsStoreState = {

    status: "idle",
    message: undefined,
    tag: null
}

/**
 * A function that runs when the user clicks to save the new attributes in TRAC.
 */
export const updateTagInTrac = createAsyncThunk<// Return type of the payload creator
    { tag: trac.metadata.ITag } | void,
    // First argument to the payload creator
    void,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('updateTagsStore/updateTagInTrac', async (_, {getState, dispatch}) => {

    // Get what we need from the store
    const {cookies: {"trac-tenant": tenant}} = getState().applicationStore
    const {tag} = getState().updateTagsStore
    const {attributes: newAttributes, attributes: {processedAttributes}} = getState().setAttributesStore.uses.updateTags

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")
    if (!tag) throw new Error("The tag for the object is not set, so the update can not be performed.")
    if (!tag.header) throw new Error("The tag for the object does not have a header, so the update can not be performed.")

    // Get the latest tag for the object trying to be updated, TRAC will error if you try and update an object
    // and do not pass a priorVersion that is the latest one. The user might get out of sync if another user
    // updates while they are editing their version of the tags.
    const latestTag = await getLatestTag({tag, tenant})

    // Check if we can proceed
    if (latestTag.header?.tagVersion !== tag.header.tagVersion) {
        const text = `You are trying to update a tag that is the not the latest version. The latest version is ${latestTag.header?.tagVersion} while you are trying to update version ${tag.header.tagVersion}. You may need to refresh the search results.`
        showToast("warning", text, "updateTagStore/updateTagInTrac/notLatestTag")
        return
    }

    // Convert the values for the SelectComponents into the TRAC attr definition
    const userSetAttributes = createTagsFromAttributes(processedAttributes, newAttributes.values)

    // Convert the original tag into Select component values, so we can work out what the user deleted and added
    const oldAttributes = {values: convertTagAttributesToSelectValues(tag, processedAttributes)}

    // We need to first get a list of which attributes are not null, if an attribute went from not null to null
    // we will delete it, if it was not set, and we now have a value, we will add it.
    const nonNullOldAttributeKeys = Object.entries(oldAttributes.values).filter(([_, value]) => Array.isArray(value) && value.length > 0 || value !== null).map(([key, _]) => key)
    const nonNullNewAttributeKeys = Object.entries(newAttributes.values).filter(([_, value]) => Array.isArray(value) && value.length > 0 || value !== null).map(([key, _]) => key)

    // Find attributes that are in the original but missing from the updated version, these will be deleted. Note that
    // There are hidden and TRAC attributes, hidden attributes should be preserved.
    const deletedAttrs: trac.metadata.ITagUpdate[] = nonNullOldAttributeKeys.filter(key => !nonNullNewAttributeKeys.includes(key)).map(key => {
        return {
            "operation": trac.TagOperation.DELETE_ATTR,
            "attrName": key,
        }
    })

    // Find the keys of attributes that have been added
    const newAttrKeys = nonNullNewAttributeKeys.filter(key => !nonNullOldAttributeKeys.includes(key))

    // We need to CREATE_OR_REPLACE_ATTR because oldAttributes have null value for all attributes if they are
    // not in the tag but are defined as being available for the object type. So if you add a new attribute
    // oldAttributes will have a null value set for it even if it is not actually in the tag.
    const newAttrs: trac.metadata.ITagUpdate[] = userSetAttributes.filter(attr => attr.attrName != undefined && newAttrKeys.includes(attr.attrName)).map(attr => {
        return {...attr, "operation": trac.TagOperation.CREATE_OR_REPLACE_ATTR}
    })

    // Attributes that were not null before and not null after and the value changes
    const changedAttrKeys = nonNullNewAttributeKeys.filter(key => nonNullOldAttributeKeys.includes(key) && !areSelectValuesEqual(oldAttributes.values[key], newAttributes.values[key]))

    // Notice that changedAttrKeys contains those attributes that are in the old and the new values, not all of these will have changed
    const changedAttrs: trac.metadata.ITagUpdate[] = userSetAttributes.filter(attr => attr.attrName != undefined && changedAttrKeys.includes(attr.attrName)).map(attr => {
        return {...attr, "operation": trac.TagOperation.REPLACE_ATTR}
    })

    // Don't update the attributes if there are no changes to them
    if (changedAttrs.concat(newAttrs).concat(deletedAttrs).length === 0) {
        showToast("warning", "No changes to the attributes were found.", "updateTagStore/updateTagInTrac/nothingChanged")
        return
    }

    // Upload the data, the response is the metadata header tag for the object, we handle both create and update here
    const tagHeader = await updateTag({
        tagUpdates: changedAttrs.concat(newAttrs).concat(deletedAttrs),
        priorVersion: tag.header,
        tenant
    })

    // Now if we get here the call to update the tag has been successful but the UI is still using the old tag
    // in both the table search results and the attribute editor. If the user clicked again then the UI would
    // error as the tag version would not be the latest. So here we get the tag for the updated object.
    const newTag: trac.metadata.ITag = await dispatch(checkForMetadata(tagHeader)).unwrap()

    // We pass through the processed objects, these are needed by the setAttributesFromTag functions which
    // we are going to call from the fulfilled endpoint of this thunk.
    return {tag: newTag}
})

export const updateTagsStore = createSlice({
    name: 'updateTagsStore',
    initialState: initialState,
    reducers: {
        /**
         * A reducer that saves the metadata tag for the object the user wants to edit.
         */
        setTag: (state, action: PayloadAction<{ tag: null | trac.metadata.ITag }>) => {

            const {tag} = action.payload

            if (tag) state.tag = tag
        }
    },
    extraReducers: (builder) => {
        // A set of lifecycle reducers to run before/after the updateTagInTrac function
        builder.addCase(updateTagInTrac.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.status = "pending"
            state.message = undefined
        })
        builder.addCase(updateTagInTrac.fulfilled, (state, action: PayloadAction<void | { tag: trac.metadata.ITag }>) => {

            state.status = "succeeded"
            state.message = undefined

            if (action.payload) {

                state.tag = action.payload.tag
                showToast("success", `The attributes were successfully updated, please refresh the search results to see the changes.`, "updateTagStore/updateTagInTrac/fulfilled")
            }
        })
        builder.addCase(updateTagInTrac.rejected, (state, action) => {

            state.status = "failed"
            state.message = undefined

            const text = {
                title: "Failed to update the attributes",
                message: "The request to update the attributes did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "updateTagsStore/updateTagInTrac/rejected")
            console.error(action.error)
        })
    }
})

// Action creators are generated for each case reducer function
export const {
    setTag
} = updateTagsStore.actions

export default updateTagsStore.reducer