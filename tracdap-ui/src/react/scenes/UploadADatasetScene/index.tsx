import ChooseSchema from "./components/ChooseSchema";
import PropTypes from "prop-types";
import React from "react";
import {SceneTitle} from "../../components/SceneTitle";
import {SelectedDataInfo} from "./components/SelectedDataInfo";
import SelectedDataInTable from "./components/SelectedDataInTable";
import {SelectFile} from "./components/SelectFile";
import {SetMetadata} from "./components/SetMetadata";
import SetPriorVersion from "./components/SetPriorVersion";
import {TextBlock} from "../../components/TextBlock";
import {UploadData} from "./components/UploadData";

/**
 * This scene allows the user to load a dataset from an Excel or CSV file. The schema can be edited and the attributes
 * set before by the user before saving the data.
 */

type Props = {

    /**
     * The main title for the page, this is set in the Menu config.
     */
    title: string
};

const LoadADatasetScene = (props: Props) => {

    console.log("Rendering LoadADatasetScene")

    const {title} = props

    return (

        <React.Fragment>
            <SceneTitle text={title}/>
            <TextBlock>
                This tool can be used to load csv or xlsx files into TRAC and save them as a dataset. These datasets
                will be available to use as model inputs but you will need to add the relevant information or attributes
                so that TRAC can find them. You will also need to confirm the schema of the dataset which includes
                information about the variable types and formats. First select the file that you want to upload.
            </TextBlock>
            <SelectFile/>
            <SelectedDataInfo/>
            <SelectedDataInTable/>
            <ChooseSchema/>
            <SetMetadata/>
            <SetPriorVersion/>
            <UploadData/>
        </React.Fragment>
    )
};

LoadADatasetScene.propTypes = {

    title: PropTypes.string.isRequired
};

export default LoadADatasetScene;