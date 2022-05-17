str = "sp cropped length skinny fit chino t.shirt"
normalizeProduct ={'jean': ['jean','boys jeans', 'men jeans', 'women jeans','denim',
"women's denim","men's denim", "girl's denim"],
 'tshirt': ['tshirt','tee shirt','tee','girls t-Shirt','t.shirt', 'm boys t-Shirt', 'm girls t-Shirt','women t-shirt',
  'men t-shirt', 'boys t-shirt',"men's tee shirt","girl's tee shirt","women's tee shirt",
  'boys t-shirts','boys t-shirt', 't-shirt', 'girls t-shirt',"boy's tee shirt",'girls t-shirt'],
   'trouser': ['trouser','men trouser', 'boys trousers','girls trousers',
   'girls trouser', 'boys trouser', 'woman trouser',
   "women's trousers", "boy's Trousers", "men's Trousers", "girl's trousers"], 
'polo': ['polo','men polo shirt', 'boys polo shirt','men polo', 'boys polo',
"men's polo shirt", "boy's polo shirt", "girl's polo shirt", "women's polo shirt",'tee polo']}

ptypeFound = False
for value in normalizeProduct.values():  
    if ptypeFound == True:
        break 
    print(value)
    print((str.lower()).split())
    print("================")
    # if value in (str.lower()).split():
    for ptype in value:
        if any(ptype in s for s in (str.lower()).split()):
            print(ptype)
            print("++++++++++++++++++++++++++++++")
            ptypeFound = True
            break
            