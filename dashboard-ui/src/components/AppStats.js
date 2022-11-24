import React, { useEffect, useState } from 'react'
import '../App.css';

export default function AppStats() {
    const [isLoaded, setIsLoaded] = useState(false);
    const [stats, setStats] = useState({});
    const [error, setError] = useState(null)

	const getStats = () => {
        fetch(`http://kafka-3855.eastus.cloudapp.azure.com/processing/stats`)
            .then(res => res.json())
            .then((result)=>{
				        console.log("Received Stats")
                setStats(result);
                setIsLoaded(true);
            },(error) =>{
                setError(error)
                setIsLoaded(true);
            })
    }
    useEffect(() => {
		const interval = setInterval(() => getStats(), 2000); // Update every 2 seconds
		return() => clearInterval(interval);
    }, [getStats]);

    if (error){
        return (<div className={"error"}>Error found when fetching from API</div>)
    } else if (isLoaded === false){
        return(<div>Loading...</div>)
    } else if (isLoaded === true){
        return(
            <div>
                <h1>Latest Stats</h1>
                <table className={"Stats"}>
					<tbody>
						<tr>
							<th>Number of Account</th>
							<th>Number of Trade</th>
						</tr>
						<tr>
							<td># account: {stats['num_account']}</td>
							<td># trade: {stats['num_trade']}</td>
						</tr>
						<tr>
							<td colspan="2">Total cash: {stats['total_cash']}</td>
						</tr>
						<tr>
							<td colspan="2">Total value: {stats['total_value']}</td>
						</tr>
						<tr>
							<td colspan="2">Total shares: {stats['total_share']}</td>
						</tr>
					</tbody>
                </table>
                <h3>Last Updated: {stats['created_at']}</h3>

            </div>
        )
    }
}
