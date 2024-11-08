// SearchComponent.js
import React, { useState, useEffect } from 'react';

function SearchComponent() {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState([]);
    const [userLogs, setUserLogs] = useState([]);
    const [recommendations, setRecommendations] = useState([]);

    const popularFurniture = [
        "Zinus Green Tea Memory Foam Mattress",
        "Walker Edison Modern Farmhouse TV Stand",
        "Furinno Simple Design Coffee Table",
        "DHP Emily Futon Sofa Bed",
        "VASAGLE Industrial Ladder Shelf",
        "Rivet Revolve Modern Upholstered Sofa",
        "Sauder Beginnings Computer Desk",
        "Novogratz Brittany Sofa Futon",
        "Hodedah Kitchen Island with Spice Rack",
        "Linenspa 8-Inch Memory Foam and Innerspring Hybrid Mattress",
        "Furinno Turn-N-Tube 5 Tier Corner Shelf",
        "Zinus Shalini Upholstered Platform Bed",
        "AmazonBasics Classic Leather-Padded Office Chair",
        "South Shore Furniture Bed with Storage",
        "DHP Twin-Over-Twin Bunk Bed",
        "Ameriwood Home Dakota L-Shaped Desk",
        "Zinus Suzanne Metal and Wood Platform Bed",
        "Furinno Econ Multipurpose Home Office Computer Writing Desk",
        "Best Choice Products Outdoor Patio Chaise Lounge Chair",
        "Zinus Mia Modern Studio Platform Bed"
    ];

    useEffect(() => {
        // 컴포넌트가 마운트될 때 localStorage에서 사용자 로그를 불러옵니다.
        const logs = JSON.parse(localStorage.getItem('userLogs')) || [];
        setUserLogs(logs);

        // 로그를 하나의 문자열로 join하여 백엔드에 요청
        if (logs.length > 0) {
            // 로그가 있으면 상위 5개를 merge하여 백엔드에 요청
            const joinedLogs = logs.join('+');
            fetchRecommendations(joinedLogs);
        }else {
            // 로그가 없을 경우 popularFurniture에서 랜덤하게 선택
            const randomFurniture = popularFurniture[Math.floor(Math.random() * popularFurniture.length)];
            fetchRecommendations(randomFurniture);
        }
    }, []);

    const fetchRecommendations = async (joinedLogs) => {
        const url = `http://localhost:8000/search`;
        const params = new URLSearchParams({ q: joinedLogs });

        try {
            const response = await fetch(`${url}?${params.toString()}`);
            const data = await response.json();
            setRecommendations(data.result.slice(0, 5)); // 상위 5개 추천 제품
        } catch (error) {
            console.error('Error fetching recommendations:', error);
        }
    };

    const handleSearch = async () => {
        const url = `http://localhost:8000/search`; // type을 "search"로 하드코딩
        const params = new URLSearchParams({ "q": query });

        try {
            const response = await fetch(`${url}?${params.toString()}`);
            console.log('response:', response);
            const data = await response.json();
            console.log('data:', data);
            setResults(data.result || []);        

            // 검색 쿼리를 사용자 로그에 추가하고 localStorage에 저장합니다.
            let newLogs = [...userLogs, query];
            if (newLogs.length > 5) {
                newLogs.shift(); // 배열의 첫 번째 요소 제거
            }
            setUserLogs(newLogs);
            localStorage.setItem('userLogs', JSON.stringify(newLogs));

        } catch (error) {
            console.error('Error fetching data:', error);
            setResults([]); // 오류 발생 시 빈 배열로 설정
        }
    };

    return (
        <div>
            <input
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="검색어를 입력하세요"
            />
            <button onClick={handleSearch}>검색</button>
            <h2>추천 제품</h2>
            <ul>
                {recommendations.map((item, index) => (
                    <li key={index}>
                        <strong>{item.item_name[0]}</strong>
                        <ul>
                            {item.bullet_point.map((point, i) => (
                                <li key={i}>{point}</li>
                            ))}
                        </ul>
                    </li>
                ))}
            </ul>
            <h2>검색 결과</h2>
            <ul>
                {results.map((item, index) => (
                    <li key={index}>
                        <strong>{item.item_name[0]}</strong>
                        <ul>
                            {item.bullet_point.map((point, i) => (
                                <li key={i}>{point}</li>
                            ))}
                        </ul>
                    </li>
                ))}
            </ul>
        </div>
    );
}

export default SearchComponent;