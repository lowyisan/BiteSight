export function extractFoodCategories(businesses) {
    const allCategories = businesses
      .flatMap(b => b.categories?.split(',').map(cat => cat.trim().toLowerCase()) || []);
  
    const categoryFrequency = {};
    allCategories.forEach(cat => {
      categoryFrequency[cat] = (categoryFrequency[cat] || 0) + 1;
    });
  
    // 🔍 Heuristic: appear in 3+ businesses + contains one of these generic roots
    const foodLike = ['food', 'restaurant', 'cafe', 'bar', 'eat', 'drink', 'halal', 'vegan', 'vegetarian', 'dessert', 'acai'];
  
    const foodCategories = Object.entries(categoryFrequency)
      .filter(([cat, count]) => {
        const hasKeyword = foodLike.some(word => cat.includes(word));
        return count >= 3 && hasKeyword;
      })
      .map(([cat]) => cat)
      .sort();
  
    return foodCategories;
  }
  