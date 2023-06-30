import {FindInTrac} from "../../components/FindInTrac";
import React from "react";
import {SceneTitle} from "../../components/SceneTitle";

/**
 * This scene allows the user to search for anything in TRAC using various search criteria and then list the search
 * results in a table.
 *
 * Note that the scene uses the FindInTrac component which is used in multiple places in the application, a key is
 * used to load up the values
 */

type Props = {

    /**
     * The main title for the page, this is set in the Menu config.
     */
    title: string,
};

const SearchScene = (props: Props) => {

    console.log("Rendering SearchScene")

    const {title} = props

    return (

        <React.Fragment>

            <SceneTitle text={title}/>

            <FindInTrac storeKey={"search"}/>

        </React.Fragment>

    )
};

export default SearchScene;