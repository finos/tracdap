import error404 from "../../images/404-error-800.jpg";
import Image from "react-bootstrap/Image";
import React from "react";

/**
 * A scene shown if the user enters a URL for an address that does not exist in the application.
 */

const Error404 = () => {

    return (

        <div className={'text-center mt-5'}>
            <Image src={error404} alt={"Oh no page not found"} width={"700px"} className={"rounded-circle"}/>
        </div>
    )
};

export default Error404;