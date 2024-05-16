/**
 * A component that shows a card with information about a menu item that the user can click on to go to that tool.
 * @module MenuCard
 * @category HomeScene Component
 */

import Card from "react-bootstrap/Card";
import {Icon} from "../../../components/Icon";
import ListGroup from "react-bootstrap/ListGroup";
import {MenuItem} from "../../../../types/types_general";
import React, {useEffect, useRef, useState} from "react";
import {useAppSelector} from "../../../../types/types_hooks";

export interface Props {

    /**
     * The function that runs when a card or a link in a card is clicked on that navigates to the right page.
     * @param event
     */
    handleCardClick: (event: React.MouseEvent) => void
    /**
     * The index of the card in the grid, those in the first row get additional layouts added above a certain
     * breakpoint.
     */
    index: number
    /**
     * The menu item being shown in the card.
     */
    menuItem: MenuItem
}

export const MenuCard = (props: Props) => {

    const {handleCardClick, index, menuItem} = props

    // Get what we need from the store
    const {application} = useAppSelector(state => state["applicationStore"].clientConfig)

    // A reference to the card outer div, so we can get its width
    const cardRef = useRef<HTMLDivElement | null>(null)

    // The width of the card that we get from the reference
    const [cardWidth, setCardWidth] = useState<number>(0)

    /**
     * A hook that runs after the page has loaded and that stores the width of the cards. This is needed to be able to
     * set the background image css for the top four cards.
     */
    useEffect(() => {
        setCardWidth(cardRef.current ? cardRef.current?.clientWidth : 0)
    }, [])

    // Are any children we should be showing, if so the layout is slightly different
    const numberOfVisibleChildren = menuItem.children.filter(subMenuItem => !subMenuItem.hiddenInHomeMenu).length

    return (

        <Card className={`h-100 homepage-card ${numberOfVisibleChildren === 0 ? "pointer" : ""}`}
              onClick={numberOfVisibleChildren === 0 ? handleCardClick : undefined}
            // The menu allows that multiple paths point to the same page, but only one of these can be shown in the menu
            // this is for the viewer components where they need to have URL parameters
              id={Array.isArray(menuItem.path) ? menuItem.path?.[0] : menuItem.path}
              ref={cardRef}
        >
            {index <= 3 &&
                // This sets a div with a gradient fill like the one used in the top banner. It lays in front of that a window onto the TRAC logo icon
                <div style={{height: `${cardWidth * 0.4}px`}} className={`d-none d-lg-block shading-mask-${application.maskColour}`}>
                    <div style={{height: `${cardWidth * 0.4}px`, backgroundSize: `${4 * cardWidth * 0.5}px`}} className={`trac-ui-card-img w-100 part-${index}`}/>
                </div>
            }

            <Card.Header className={"text-center"}>
                <div className={"trac-ui-card-icon d-flex m-auto justify-content-center"}>
                    <Icon ariaLabel={false}

                          icon={menuItem.icon || "bi-hexagon"}
                          size={"2.5rem"}
                    />
                </div>
                <div className={"trac-ui-card-title fs-4"}>{menuItem.title}</div>
            </Card.Header>
            <Card.Body>
                <Card.Text>
                    {menuItem.description}
                </Card.Text>

                {numberOfVisibleChildren > 0 &&
                    <ListGroup className="list-group-flush">
                        {menuItem.children.filter(subMenuItem => !subMenuItem.hiddenInHomeMenu).map(subMenuItem => (

                            <ListGroup.Item className={"pointer fs-13"}
                                            id={`${menuItem.path}/${subMenuItem.path}`}
                                            key={`${menuItem.path}/${subMenuItem.path}`}
                                            onClick={handleCardClick}>
                                <Icon ariaLabel={false}
                                      className={"me-2"}
                                      icon={"bi-hexagon"}
                                      size={"0.875rem"}
                                />
                                {subMenuItem.title}
                            </ListGroup.Item>

                        ))}
                    </ListGroup>
                }

            </Card.Body>
            {menuItem.children.filter(child => !child.hiddenInHomeMenu).length === 0 &&
                <Card.Footer className="text-muted">&nbsp;</Card.Footer>
            }
        </Card>
    )
};