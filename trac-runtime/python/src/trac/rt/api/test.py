# Let us assume this CONFIG holds some sensitive information
CONFIG = {
    "KEY": "ASXFYFGK78989"
}
  
class PeopleInfo:
    def __init__(self, fname, lname):
        self.fname = fname
        self.lname = lname
  
def get_name_for_avatar(avatar_str, people_obj):
    return avatar_str.format(people_obj = people_obj)
  
  
# Driver Code
people = PeopleInfo('GEEKS', 'FORGEEKS')
  
# case 1: st obtained from user
st = input()
get_name_for_avatar(st, people_obj = people)