/**
 * A component that shows a splash banner at the top along with application and client logos.
 *
 * @module TopBanner
 * @category Component
 */

import Container from "react-bootstrap/Container";
import Image from "react-bootstrap/Image";
import React from "react";
import {useAppSelector} from "../../types/types_hooks";

export const TopBanner = () => {

    // Get what we need from the store
    const {"trac-theme": theme} = useAppSelector(state => state["applicationStore"].cookies)
    const {images, application} = useAppSelector(state => state["applicationStore"].clientConfig)

    return (

        <div id="top-banner" className={theme}>
            <div className={`theme shading-mask-${application.maskColour}`}>
                <Container className="h-100">
                    <div className="h-100 d-flex justify-content-between align-items-center">

                        <div className={"d-flex align-items-center"}>
                            <Image style={images.application.darkBackground.style}
                                   width={images.application.darkBackground.displayWidth}
                                   height={images.application.darkBackground.displayHeight}
                                   src={images.application.darkBackground.src}
                                   alt={images.application.darkBackground.alt}
                            />

                            <div className={"d-flex flex-column lh-1"}>
                            <span className={`banner-logo font-tracdap title ms-3 fw-bolder`}>{application.name}</span>
                            {application.tagline && <span className={`banner-logo font-tracdap tagline ms-3`}>{application.tagline}</span>}
                            </div>
                        </div>

                        <Image className={"d-none d-md-flex"}
                               style={images.client.darkBackground.style}
                               width={images.client.darkBackground.displayWidth}
                               height={images.client.darkBackground.displayHeight}
                               src={images.client.darkBackground.src}
                               alt={images.client.darkBackground.alt}
                        />

                    </div>
                </Container>
            </div>
        </div>
    )
};