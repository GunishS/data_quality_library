class median:
    def __init__(self,num_list):
            self.num_list = num_list
            pass

    def calculate_median(self):
        sorted_list = sorted(self.num_list)
            # odd
        if len(sorted_list) % 2 != 0:
            med = sorted_list[len(sorted_list)// 2] 
            return med
        else : 
            # even 
            med = (sorted_list[len(sorted_list)//2] + sorted_list[(len(sorted_list)//2) - 1]) / 2 
            return med

    def calculate_iqr(self):
        sorted_list = sorted(self.num_list)
        # print(sorted_list)
        sorted_list = median(sorted_list)
        med = sorted_list.calculate_median()
        # print(sorted_list) # obj create huya h so memory location print hoga 
        sorted_list = sorted_list.num_list # uss object ka num_list vla attribute access kr rha hu jo ke ek array h 
        # print(sorted_list) # array print hoga yha
        # odd no of elements in the list
        if (len(sorted_list) % 2) != 0 :
            sorted_list_q1 = sorted_list[0 : (len(sorted_list)//2 )]
            sorted_list_q3 = sorted_list[(len(sorted_list)//2 + 1) : ]

            sorted_list_q1 = median(sorted_list_q1)
            # sorted_list_q1 = sorted_list_q1.num_list
            sorted_list_q3 = median(sorted_list_q3)
            # sorted_list_q3 = sorted_list_q3.num_list
            # print(sorted_list_q1,sorted_list_q3)

            q1 = sorted_list_q1.calculate_median()
            q3 = sorted_list_q3.calculate_median()
            return [q1,med,q3]
        # even no of elements in the list
        else : 
            sorted_list_q1 = sorted_list[0 : (len(sorted_list)//2 )]
            # print(sorted_list_q1)
            sorted_list_q3 = sorted_list[(len(sorted_list)//2) : ]
            sorted_list_q1 = median(sorted_list_q1)
            # sorted_list_q1 = sorted_list_q1.num_list
            sorted_list_q3 = median(sorted_list_q3)
            q1 = sorted_list_q1.calculate_median()
            q3 = sorted_list_q3.calculate_median()
            return [q1,med,q3]
        


num_list = [10,20,30,40,50,60]

med_1 = median(num_list)
# median_value = med_1.calculate_median()
median_value = med_1.calculate_iqr()
print(median_value)