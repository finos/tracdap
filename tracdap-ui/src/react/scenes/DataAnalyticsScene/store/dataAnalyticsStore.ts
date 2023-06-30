import {createSlice, PayloadAction} from '@reduxjs/toolkit'
import {tracdap as trac} from "@finos/tracdap-web-api";

// Define a type for the slice state
type FindInTracStoreState = {

    selectedData: {
        tags: trac.metadata.ITag[]
    }
}

const initialState: FindInTracStoreState = {

    selectedData: {
        tags: [],
    }
}

export const DataAnalyticsStore = createSlice({
    name: 'dataAnalyticsStore',
    initialState: initialState,
    reducers: {
        /**
         * A reducer that stores the metadata tags for the datasets selected by the user. This is activated by a button in the ChooseADatasetModal
         * component. Although this is just an action that copies the data from the FindInTrac component to this store we do this so that it runs
         * when the user clicks a button to 'save' their selection. If instead we passed the value directly in the findInTracStore to the
         * DataAnalyticsScene then as the user clicked around the whole DataAnalyticsScene would be updating with each click.
         */
        setSelectedTags: (state, action: PayloadAction<trac.metadata.ITag[]>) => {

            state.selectedData.tags = action.payload
        }
    }
})

// Action creators are generated for each case reducer function
export const {
   setSelectedTags
} = DataAnalyticsStore.actions

export default DataAnalyticsStore.reducer