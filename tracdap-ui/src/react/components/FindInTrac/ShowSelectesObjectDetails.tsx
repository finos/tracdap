/**
 * A component that allows the user to see a summary table of each of the rows they have selected from the
 * search result. If more than one result is selected then a carousel is shown with the user able to click
 * between them.
 */
import Carousel from "react-bootstrap/Carousel";
import {type FindInTracStoreState} from "./findInTracStore";
import {ObjectDetails} from "../ObjectDetails";
import React from "react";
import {ShowHideDetails} from "../ShowHideDetails";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the ShowSelectedObjectDetails component.
 */
export interface Props {

    /**
     *  The key in the FindInTracStore to use to save the data to/ get the data from.
     */
    storeKey: keyof FindInTracStoreState["uses"]
}

export const ShowSelectedObjectDetails = (props: Props) => {

    const {storeKey} = props

    console.log("Rendering ShowSelectedObjectDetails")

    // Because this component includes a table we want to pay attention to the optimisation, if we avoid destructuring the
    // strings we avoid getting a rerender
    const selectedTab = useAppSelector(state => state["findInTracStore"].uses[storeKey].selectedTab)
    const tags = useAppSelector(state => state["findInTracStore"].uses[storeKey].selectedTags[selectedTab])

    return (

        <React.Fragment>

            {tags.length > 0 &&
                <ShowHideDetails linkText={"summary information"} showOnOpen={true}>
                    <Carousel controls={Boolean(tags.length > 1)}
                              indicators={Boolean(tags.length > 1 && tags.length < 15)}
                              interval={null}
                              variant="dark"
                    >
                        {tags.map((tag, i) => (
                            // The "px-5" is to make the control arrows show outside the table, there is a Boostrap scss variable that
                            // controls their width
                            <Carousel.Item key={tag.header?.objectId || i} className={`${tags.length < 2 ? "" : "px-5"}`}>
                                <ObjectDetails metadata={tag} bordered={false} striped={true}/>
                            </Carousel.Item>
                        ))}
                    </Carousel>
                </ShowHideDetails>
            }

        </React.Fragment>
    )
};